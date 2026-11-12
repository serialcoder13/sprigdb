//! Stored code keeps its tables in place (chapter 16, "Stored code keeps
//! its tables in place"; decision bugfix-31).
//!
//! A view, procedure, function, package, trigger or event lives in one
//! MariaDB schema and reads its tables there, by unqualified names (or by
//! the physical names chapter 07's rewrite put in its text). The first
//! design's regroup moves tables into their group's schema with `RENAME
//! TABLE`, and the group's shards later split, merge and move to other
//! clusters; code left behind then fails with 1146 for every user, and no
//! rewrite of its text survives a split (one schema can't name a table
//! spread over two) or a move (MariaDB code can't read another server).
//! So the advisor never moves a table out from under stored code:
//!
//! - a table that a view, procedure, function, package or event names
//!   stays in its database's default schema (an implied `UNSHARDED` pin,
//!   with a line in the run's evidence saying which code names it);
//! - a trigger moves with its table, so its table may move only together
//!   with every table its body names (the same group, or neither moves:
//!   [`Input::code_couples`], settled by [`settle`]); a trigger that calls
//!   a stored routine keeps its table in place (the routine stays behind);
//! - dynamic SQL that isn't one string literal (`PREPARE s FROM @q`,
//!   `EXECUTE IMMEDIATE CONCAT(...)`) may name any table of its schema:
//!   every table of that schema stays;
//! - a key-value unit (a key table and its element tables) stays whole.
//!
//! Row-level security's own objects (`__sprig_rls*`: policy views, their
//! context function and write-check triggers) move with their table at the
//! regroup (decision RL29) and are not counted. [`conflicts`] checks a
//! stored design against the code that exists when it is applied.

use std::collections::{BTreeMap, BTreeSet};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::pins::{Pin, PinKind};
use sprig_sql::lex::LexOptions;
use sprig_sql::stored_refs::references;

use super::design::{Design, Input, Obj, SearchResult};
use super::input::Located;

/// What kind of stored code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CodeKind {
    /// A view.
    View,
    /// A procedure.
    Procedure,
    /// A function.
    Function,
    /// A package or package body (`sql_mode=ORACLE`).
    Package,
    /// A trigger.
    Trigger,
    /// An event.
    Event,
}

impl CodeKind {
    /// The word for the evidence.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Procedure => "procedure",
            Self::Function => "function",
            Self::Package => "package",
            Self::Trigger => "trigger",
            Self::Event => "event",
        }
    }
}

/// One stored program or view of the tenant on its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCode {
    /// Its schema.
    pub schema: String,
    /// What it is.
    pub kind: CodeKind,
    /// Its name.
    pub name: String,
    /// A trigger's table.
    pub table: Option<String>,
    /// Its body (a view: its definition).
    pub text: String,
    /// The `sql_mode` it was created with (empty for views).
    pub sql_mode: String,
}

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, format!("{what}: {e}"))
}

/// The tenant's stored code on the connection's server, from
/// `information_schema` (as `sprig_ctl`, which sees every body).
///
/// # Errors
/// MariaDB's.
pub async fn read(c: &mut Conn, tenant: TenantId) -> Result<Vec<StoredCode>> {
    let like = format!("t{tenant}\\_d%");
    let mut out = Vec::new();
    let routines: Vec<(String, String, String, Option<String>, String)> = c
        .exec(
            "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION, SQL_MODE \
             FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA LIKE ?",
            (&like,),
        )
        .await
        .map_err(|e| db_err("stored routines", &e))?;
    for (schema, name, kind, text, sql_mode) in routines {
        let kind = match kind.to_ascii_uppercase().as_str() {
            "PROCEDURE" => CodeKind::Procedure,
            "FUNCTION" => CodeKind::Function,
            _ => CodeKind::Package,
        };
        out.push(StoredCode {
            schema,
            kind,
            name,
            table: None,
            text: text.unwrap_or_default(),
            sql_mode,
        });
    }
    // Views and triggers one schema at a time: a LIKE on their schema
    // reads every table of every tenant on the server (R9, r9scale;
    // `sprig_catalog::infoschema`).
    let schemas = sprig_catalog::infoschema::schemas_like(c, &like).await?;
    let views: Vec<(String, String, Option<String>)> = sprig_catalog::infoschema::read_per_schema(
        c,
        "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS",
        "TABLE_SCHEMA",
        &schemas,
        "",
    )
    .await?;
    for (schema, name, text) in views {
        out.push(StoredCode {
            schema,
            kind: CodeKind::View,
            name,
            table: None,
            text: text.unwrap_or_default(),
            sql_mode: String::new(),
        });
    }
    let triggers: Vec<(String, String, String, Option<String>, String)> =
        sprig_catalog::infoschema::read_per_schema(
            c,
            "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT, SQL_MODE \
             FROM information_schema.TRIGGERS",
            "EVENT_OBJECT_SCHEMA",
            &schemas,
            "",
        )
        .await?;
    for (schema, name, table, text, sql_mode) in triggers {
        out.push(StoredCode {
            schema,
            kind: CodeKind::Trigger,
            name,
            table: Some(table),
            text: text.unwrap_or_default(),
            sql_mode,
        });
    }
    let events: Vec<(String, String, Option<String>, String)> = c
        .exec(
            "SELECT EVENT_SCHEMA, EVENT_NAME, EVENT_DEFINITION, SQL_MODE \
             FROM information_schema.EVENTS WHERE EVENT_SCHEMA LIKE ?",
            (&like,),
        )
        .await
        .map_err(|e| db_err("events", &e))?;
    for (schema, name, text, sql_mode) in events {
        out.push(StoredCode {
            schema,
            kind: CodeKind::Event,
            name,
            table: None,
            text: text.unwrap_or_default(),
            sql_mode,
        });
    }
    out.sort_by(|a, b| (&a.schema, a.kind, &a.name).cmp(&(&b.schema, b.kind, &b.name)));
    Ok(out)
}

/// What stored code requires of a design.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodeRules {
    /// Tables that stay in their default schema, with why.
    pub held: BTreeMap<Obj, Vec<String>>,
    /// Tables that move only together (a trigger's table and a table its
    /// body names), with why.
    pub couples: Vec<(Obj, Obj, String)>,
}

fn lex_for(sql_mode: &str) -> LexOptions {
    let up = sql_mode.to_ascii_uppercase();
    LexOptions::mariadb()
        .ansi_quotes(up.contains("ANSI_QUOTES") || up.contains("ORACLE"))
        .no_backslash_escapes(up.contains("NO_BACKSLASH_ESCAPES"))
}

/// The rules `code` sets for the tables in `located` (see the module docs).
#[must_use]
pub fn rules(code: &[StoredCode], located: &[Located]) -> CodeRules {
    let mut r = CodeRules::default();
    let find = |schema: &str, name: &str| {
        located
            .iter()
            .find(|l| l.schema.eq_ignore_ascii_case(schema) && l.name.eq_ignore_ascii_case(name))
            .map(|l| l.object)
    };
    let name_of = |o: Obj| {
        located
            .iter()
            .find(|l| l.object == o)
            .map_or_else(|| o.to_string(), |l| l.name.clone())
    };
    // Routine names per schema (lower case), for calls from triggers.
    let mut routines: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for c in code {
        if matches!(
            c.kind,
            CodeKind::Procedure | CodeKind::Function | CodeKind::Package
        ) {
            routines
                .entry(c.schema.to_ascii_lowercase())
                .or_default()
                .insert(c.name.to_ascii_lowercase());
        }
    }
    let mut hold = |o: Obj, why: String| {
        let v = r.held.entry(o).or_default();
        if !v.contains(&why) {
            v.push(why);
        }
    };
    let mut couples: Vec<(Obj, Obj, String)> = Vec::new();
    for c in code {
        if c.name.to_ascii_lowercase().starts_with("__sprig_rls") {
            continue;
        }
        let refs = references(&c.text, &lex_for(&c.sql_mode));
        let what = format!("{} {}", c.kind.word(), c.name);
        let named: BTreeSet<Obj> = refs
            .tables
            .iter()
            .filter_map(|(s, t)| find(s.as_deref().unwrap_or(&c.schema), t))
            .collect();
        let own = c
            .table
            .as_deref()
            .and_then(|t| find(&c.schema, t))
            .filter(|_| c.kind == CodeKind::Trigger);
        if refs.opaque {
            let why = format!(
                "{what} runs dynamic SQL whose statements can't be read, so every table of its \
                 schema stays"
            );
            for l in located
                .iter()
                .filter(|l| l.schema.eq_ignore_ascii_case(&c.schema))
            {
                hold(l.object, why.clone());
            }
        }
        match own {
            Some(t) => {
                let calls: Vec<String> = refs
                    .calls
                    .iter()
                    .filter_map(|(q, n)| {
                        let in_schema = |s: &str, x: &str| {
                            routines
                                .get(&s.to_ascii_lowercase())
                                .is_some_and(|set| set.contains(&x.to_ascii_lowercase()))
                        };
                        // `s.f(`: a routine of schema `s`; `pkg.f(`: a
                        // package of the trigger's schema.
                        let hit = match q {
                            Some(q) => in_schema(q, n) || in_schema(&c.schema, q),
                            None => in_schema(&c.schema, n),
                        };
                        hit.then(|| n.clone())
                    })
                    .collect();
                if !calls.is_empty() {
                    hold(
                        t,
                        format!(
                            "its {what} calls {}, which stays in the default schema",
                            calls.join(", ")
                        ),
                    );
                }
                for u in named.iter().copied().filter(|u| *u != t) {
                    couples.push((
                        t,
                        u,
                        format!("{what} on {} names {}", name_of(t), name_of(u)),
                    ));
                }
            }
            None => {
                for o in named {
                    hold(o, format!("{what} names it"));
                }
            }
        }
    }
    r.couples = couples;
    r
}

/// Keeps `o` in its default schema: an implied `UNSHARDED` pin (replacing
/// any client pin, which waits until the code no longer names the table)
/// and a line of evidence. Returns whether it wasn't held before.
fn hold(input: &mut Input, o: Obj, why: &[String]) -> bool {
    if input.held.contains(&o) {
        return false;
    }
    let Some(t) = input.tables.iter().find(|t| t.object == o) else {
        return false;
    };
    let (name, db) = (t.name.clone(), t.db);
    let before = input
        .pins
        .iter()
        .position(|p| p.object.0 == o)
        .map(|i| input.pins.remove(i).kind);
    let pinned = match before {
        Some(PinKind::Unsharded) | None => String::new(),
        Some(PinKind::Reference) => "; its pin REFERENCE waits".to_owned(),
        Some(PinKind::ShardKey { .. }) => "; its pin SHARD KEY waits".to_owned(),
        Some(PinKind::Colocate { .. }) => "; its pin COLOCATE WITH waits".to_owned(),
    };
    input.pins.push(Pin {
        object: ObjectId(o),
        db: DbId(db),
        kind: PinKind::Unsharded,
    });
    input.pins.sort_by_key(|p| p.object);
    input.held.push(o);
    input.notes.push(format!(
        "{name}: stays unsharded because stored code reads it where it is: {}{pinned}",
        why.join("; ")
    ));
    true
}

/// Applies `rules` to `input`: holds the tables (with a key-value unit's
/// tables together) and records the trigger couples for [`settle`].
pub fn apply_rules(input: &mut Input, rules: &CodeRules) {
    let mut held: Vec<(Obj, Vec<String>)> =
        rules.held.iter().map(|(o, w)| (*o, w.clone())).collect();
    // A key-value unit stays whole.
    let units: Vec<Vec<Obj>> = input
        .kv
        .iter()
        .map(|u| {
            let mut v = vec![u.key];
            v.extend(u.elements.iter().copied());
            v
        })
        .collect();
    let mut extra = Vec::new();
    for (o, _) in &held {
        for u in units.iter().filter(|u| u.contains(o)) {
            for m in u.iter().filter(|m| *m != o && !rules.held.contains_key(m)) {
                extra.push((*m, vec![format!("its key-value unit's table {o} stays")]));
            }
        }
    }
    held.extend(extra);
    for (o, why) in held {
        hold(input, o, &why);
    }
    input.code_couples.clone_from(&rules.couples);
}

/// The group (index) holding `o` in `d`, `None` when it stays unsharded.
fn group_of(d: &Design, o: Obj) -> Option<usize> {
    d.groups
        .iter()
        .position(|g| g.members.iter().any(|m| m.object == o))
}

/// Runs `find` (the structural design or the search) until no trigger
/// couple is split: a couple whose tables land apart is held in place and
/// the design is found again. Each pass holds at least one more table, so
/// it ends.
pub fn settle(input: &mut Input, find: impl Fn(&Input) -> SearchResult) -> SearchResult {
    loop {
        let found = find(input);
        let apart: Vec<(Obj, Obj, String)> = input
            .code_couples
            .iter()
            .filter(|(a, b, _)| group_of(&found.best, *a) != group_of(&found.best, *b))
            .cloned()
            .collect();
        let mut more = false;
        for (a, b, why) in apart {
            let w = vec![format!("{why}, and they would be in different groups")];
            more |= hold(input, a, &w);
            more |= hold(input, b, &w);
        }
        if !more {
            return found;
        }
    }
}

/// What in `design` the stored code now in `rules` forbids: tables it
/// would move that code keeps in place, and trigger couples it would
/// split.
#[must_use]
pub fn conflicts(design: &Design, rules: &CodeRules, located: &[Located]) -> Vec<String> {
    let name = |o: Obj| {
        located
            .iter()
            .find(|l| l.object == o)
            .map_or_else(|| o.to_string(), |l| l.name.clone())
    };
    let mut out = Vec::new();
    for (o, why) in &rules.held {
        if group_of(design, *o).is_some() {
            out.push(format!("{} ({})", name(*o), why.join("; ")));
        }
    }
    for (a, b, why) in &rules.couples {
        if group_of(design, *a) != group_of(design, *b) {
            out.push(format!("{} and {} ({why})", name(*a), name(*b)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::sharding::design::{Column, GroupDesign, Member, Routing, Table, structural};
    use crate::sharding::settings::AdvisorSettings;
    use sprig_shard::KeyClass;

    const S: &str = "t9_d1_g1_s1";

    fn at(o: Obj, name: &str) -> Located {
        Located {
            object: o,
            db: DbId(1),
            schema: S.into(),
            name: name.into(),
        }
    }

    fn located() -> Vec<Located> {
        vec![
            at(1, "la"),
            at(2, "la_log"),
            at(3, "cust"),
            at(4, "ord"),
            at(5, "free"),
        ]
    }

    fn code(kind: CodeKind, name: &str, table: Option<&str>, text: &str) -> StoredCode {
        StoredCode {
            schema: S.into(),
            kind,
            name: name.into(),
            table: table.map(str::to_owned),
            text: text.into(),
            sql_mode: "STRICT_TRANS_TABLES".into(),
        }
    }

    #[test]
    fn code_holds_what_it_names() {
        let c = vec![
            code(
                CodeKind::Procedure,
                "pcount",
                None,
                "SELECT COUNT(*) FROM la",
            ),
            code(
                CodeKind::View,
                "vla",
                None,
                "select `t9_d1_g1_s1`.`la`.`k` AS `k` from `t9_d1_g1_s1`.`la`",
            ),
            code(
                CodeKind::Event,
                "ev",
                None,
                "INSERT INTO la_log (n) SELECT COUNT(*) FROM la",
            ),
            code(CodeKind::Function, "f42", None, "RETURN 42"),
            // A trigger naming another table: a couple, not a hold.
            code(
                CodeKind::Trigger,
                "tr_ord",
                Some("ord"),
                "INSERT INTO cust (id, v) VALUES (NEW.id, 'x')",
            ),
            // A trigger touching only its row.
            code(
                CodeKind::Trigger,
                "tr_free",
                Some("free"),
                "SET NEW.v = UPPER(NEW.v)",
            ),
            // Row-level security's own objects move with their table.
            code(
                CodeKind::View,
                "__sprig_rls_cust",
                None,
                "select * from `t9_d1_g1_s1`.`cust`",
            ),
        ];
        let r = rules(&c, &located());
        assert_eq!(r.held.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert!(r.held[&1].iter().any(|w| w == "procedure pcount names it"));
        assert!(r.held[&1].iter().any(|w| w == "view vla names it"));
        assert!(r.held[&2].iter().any(|w| w == "event ev names it"));
        assert_eq!(r.couples.len(), 1);
        assert_eq!((r.couples[0].0, r.couples[0].1), (4, 3));
    }

    #[test]
    fn triggers_calling_routines_and_opaque_dynamic_sql_hold() {
        let c = vec![
            code(CodeKind::Function, "clamp", None, "RETURN LEAST(x, 10)"),
            code(
                CodeKind::Trigger,
                "tr",
                Some("cust"),
                "SET NEW.v = clamp(NEW.v)",
            ),
            code(
                CodeKind::Procedure,
                "pdyn",
                None,
                "BEGIN SET @q = CONCAT('SELECT 1 FROM ', 'la'); PREPARE s FROM @q; EXECUTE s; END",
            ),
        ];
        let r = rules(&c, &located());
        assert!(r.held[&3][0].contains("calls clamp"), "{:?}", r.held);
        // Opaque dynamic SQL holds every table of its schema.
        assert_eq!(r.held.len(), 5);
        // A literal one holds only what it names.
        let c = vec![code(
            CodeKind::Procedure,
            "plit",
            None,
            "BEGIN PREPARE s FROM 'SELECT COUNT(*) FROM free'; EXECUTE s; END",
        )];
        let r = rules(&c, &located());
        assert_eq!(r.held.keys().copied().collect::<Vec<_>>(), vec![5]);
    }

    fn table(o: Obj, name: &str) -> Table {
        Table {
            object: o,
            db: 1,
            name: name.into(),
            columns: vec![Column {
                name: "id".into(),
                class: Some(KeyClass::Numeric),
                ndv: Some(1e6),
                top_share: None,
                sql: None,
            }],
            pk: vec!["id".into()],
            uniques: Vec::new(),
            size_bytes: 1e9,
        }
    }

    #[test]
    fn held_tables_stay_out_of_groups_and_couples_settle() {
        let mut input = Input {
            tables: vec![
                table(1, "la"),
                table(2, "la_log"),
                table(3, "cust"),
                table(4, "ord"),
                table(5, "free"),
            ],
            split_bytes: 1e8,
            ..Input::default()
        };
        let c = vec![
            code(
                CodeKind::Procedure,
                "pcount",
                None,
                "SELECT COUNT(*) FROM la",
            ),
            code(
                CodeKind::Trigger,
                "tr_ord",
                Some("ord"),
                "INSERT INTO cust (id) VALUES (NEW.id)",
            ),
        ];
        let r = rules(&c, &located());
        apply_rules(&mut input, &r);
        let s = AdvisorSettings {
            min_ndv_per_shard: 0,
            ..AdvisorSettings::default()
        };
        let found = settle(&mut input, |i| {
            let (d, evidence) = structural(i, &s);
            SearchResult {
                best: d,
                score: crate::sharding::design::Score::default(),
                path: Vec::new(),
                alternatives: Vec::new(),
                evidence,
            }
        });
        let d = &found.best;
        assert_eq!(group_of(d, 1), None, "{d:?}");
        // ord and cust have no foreign key: separate groups, so the
        // trigger's couple holds both.
        assert_eq!(group_of(d, 3), None, "{d:?}");
        assert_eq!(group_of(d, 4), None, "{d:?}");
        assert!(group_of(d, 5).is_some(), "{d:?}");
        assert!(
            found
                .evidence
                .iter()
                .any(|e| e.starts_with("la: stays unsharded because stored code")),
            "{:?}",
            found.evidence
        );
        assert!(
            !found
                .evidence
                .iter()
                .any(|e| e.contains("pinned UNSHARDED")),
            "{:?}",
            found.evidence
        );
        // A stored design that moves la conflicts with the code.
        let moved = Design {
            groups: vec![GroupDesign {
                name: "la".into(),
                key: vec![KeyClass::Numeric],
                members: vec![Member {
                    object: 1,
                    routing: Routing::Columns {
                        columns: vec!["id".into()],
                    },
                }],
            }],
            unsharded: Vec::new(),
        };
        let why = conflicts(&moved, &r, &located());
        assert_eq!(why.len(), 1, "{why:?}");
        assert!(why[0].starts_with("la (procedure pcount"), "{why:?}");
    }
}
