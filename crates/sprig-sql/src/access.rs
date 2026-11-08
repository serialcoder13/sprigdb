//! What a statement touches, for the gateway's privilege check (chapter
//! 29, "Evaluation": SQL is checked coarsely by the gateway and exactly by
//! MariaDB).
//!
//! [`requirements`] parses one data statement with sqlparser-rs (from the
//! same parse view as chapter 07's AST path) and lists each relation it
//! names with the actions that could make the statement legal there:
//!
//! | Statement | Target | Every other relation |
//! |---|---|---|
//! | `SELECT`, `WITH`, `TABLE` | | `SELECT` |
//! | `INSERT` | `INSERT` (`REPLACE`: and `DELETE`; `ON DUPLICATE KEY UPDATE`: and `UPDATE`) | `SELECT` |
//! | single-table `UPDATE` | `UPDATE` | `SELECT` |
//! | multi-table `UPDATE` | each joined table: `UPDATE` or `SELECT` | `SELECT` |
//! | `DELETE` | each target: `DELETE` | `SELECT` |
//!
//! [`ddl_requirements`] does the same for a classified DDL statement. The
//! lists never ask for more than MySQL would (a column read inside an
//! `UPDATE`'s `SET` is MariaDB's to check); `None` means the statement
//! wasn't modelled or didn't parse, and the gateway then leaves it to
//! MariaDB and the user's backend account.

use std::collections::HashMap;
use std::ops::ControlFlow;

use sprig_auth::authz::{Action, ActionSet, ObjectType};
use sqlparser::ast::{
    FromTable, ObjectName, ObjectNamePart, OnInsert, Query, Statement, TableFactor, TableObject,
    TableWithJoins, Visit, Visitor,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::lex::{self, LexOptions, Token, TokenKind};
use crate::names::ddl::AlterOp;
use crate::names::{DdlKind, DdlName, DdlTarget};

/// One thing a statement needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// A table or view: any one of `any`, and every one of `all`.
    Table {
        /// The database, when the statement qualified the name.
        db: Option<String>,
        /// The table.
        name: String,
        /// Any one of these is enough.
        any: ActionSet,
    },
    /// A database-level action (creating objects in it, dropping it).
    Database {
        /// The database; `None` for the current one.
        db: Option<String>,
        /// The action.
        action: Action,
        /// The object being created, when the refusal names it (MySQL's
        /// 1142 for `CREATE TABLE`), else `None` (1044).
        object: Option<String>,
    },
    /// A routine.
    Routine {
        /// The database; `None` for the current one.
        db: Option<String>,
        /// The routine.
        name: String,
        /// Function or procedure.
        kind: ObjectType,
        /// The action.
        action: Action,
    },
}

fn table(name: &ObjectName, any: ActionSet) -> Option<Need> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [t] => Some(Need::Table {
            db: None,
            name: (*t).to_owned(),
            any,
        }),
        [d, t] => Some(Need::Table {
            db: Some((*d).to_owned()),
            name: (*t).to_owned(),
            any,
        }),
        _ => None,
    }
}

/// Relations, CTE names and aliases in one statement.
#[derive(Default)]
struct Collect {
    relations: Vec<ObjectName>,
    ctes: Vec<String>,
    aliases: HashMap<String, ObjectName>,
}

impl Visitor for Collect {
    type Break = ();

    fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
        self.relations.push(r.clone());
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.ctes.push(c.alias.name.value.to_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name,
            alias: Some(a),
            ..
        } = t
        {
            self.aliases
                .insert(a.name.value.to_lowercase(), name.clone());
        }
        ControlFlow::Continue(())
    }
}

fn single(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(i)] => Some(i.value.to_lowercase()),
        _ => None,
    }
}

fn tables_of(t: &TableWithJoins) -> Vec<ObjectName> {
    std::iter::once(&t.relation)
        .chain(t.joins.iter().map(|j| &j.relation))
        .filter_map(|f| match f {
            TableFactor::Table { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Removes one occurrence of `n` from `v`.
fn take(v: &mut Vec<ObjectName>, n: &ObjectName) {
    if let Some(i) = v.iter().position(|x| x == n) {
        v.remove(i);
    }
}

/// The relations `sql` (one data statement, as the client wrote it) names,
/// with the actions each needs; `None` when it isn't modelled.
#[must_use]
pub fn requirements(sql: &[u8], opts: &LexOptions) -> Option<Vec<Need>> {
    let text = std::str::from_utf8(sql).ok()?;
    let tokens = lex::lex(sql, opts).ok()?;
    let toks: Vec<Token> = tokens.clone();
    let view = parse_view(text, &toks)?;
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, &view).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let stmt = stmts.pop()?;
    let mut c = Collect::default();
    let _ = stmt.visit(&mut c);
    let select = ActionSet::of(&[Action::Select]);
    let mut needs = Vec::new();
    let mut rest = std::mem::take(&mut c.relations);
    match &stmt {
        Statement::Query(_) => {}
        Statement::Insert(ins) => {
            let TableObject::TableName(target) = &ins.table else {
                return None;
            };
            take(&mut rest, target);
            let mut any = ActionSet::of(&[Action::Insert]);
            let mut extra = Vec::new();
            if ins.replace_into {
                extra.push(Action::Delete);
            }
            if matches!(ins.on, Some(OnInsert::DuplicateKeyUpdate(_))) {
                extra.push(Action::Update);
            }
            needs.push(table(target, any)?);
            for a in extra {
                any = ActionSet::of(&[a]);
                needs.push(table(target, any)?);
            }
        }
        Statement::Update(u) => {
            let targets = tables_of(&u.table);
            let single_table = targets.len() == 1 && u.from.is_none();
            for t in &targets {
                take(&mut rest, t);
                let any = if single_table {
                    ActionSet::of(&[Action::Update])
                } else {
                    ActionSet::of(&[Action::Update, Action::Select])
                };
                needs.push(table(t, any)?);
            }
        }
        Statement::Delete(d) => {
            let from = match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            if d.tables.is_empty() {
                let [only] = from.as_slice() else {
                    return None;
                };
                let targets = tables_of(only);
                let [t] = targets.as_slice() else {
                    return None;
                };
                take(&mut rest, t);
                needs.push(table(t, ActionSet::of(&[Action::Delete]))?);
            } else {
                for t in &d.tables {
                    take(&mut rest, t);
                    let real = single(t)
                        .and_then(|a| c.aliases.get(&a).cloned())
                        .unwrap_or_else(|| t.clone());
                    take(&mut rest, &real);
                    needs.push(table(&real, ActionSet::of(&[Action::Delete]))?);
                }
            }
        }
        _ => return None,
    }
    for r in rest {
        if let Some(s) = single(&r)
            && c.ctes.contains(&s)
        {
            continue;
        }
        needs.push(table(&r, select)?);
    }
    Some(needs)
}

fn db_of(n: &DdlName) -> Option<String> {
    n.db.clone()
}

/// What a classified DDL statement needs (chapter 29's action table:
/// creating objects needs `CREATE` on the database, altering and dropping
/// them the action on the object, routines `CREATE ROUTINE` and `ALTER
/// ROUTINE`, triggers `TRIGGER` on the table).
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per DDL shape
pub fn ddl_requirements(t: &DdlTarget) -> Vec<Need> {
    let tbl = |n: &DdlName, a: &[Action]| Need::Table {
        db: db_of(n),
        name: n.name.clone(),
        any: ActionSet::of(a),
    };
    let dbn = |db: Option<String>, action: Action| Need::Database {
        db,
        action,
        object: None,
    };
    let dbo = |n: &DdlName, action: Action| Need::Database {
        db: db_of(n),
        action,
        object: Some(n.name.clone()),
    };
    let routine = |kind: DdlKind| match kind {
        DdlKind::Function => ObjectType::Function,
        _ => ObjectType::Procedure,
    };
    match t {
        DdlTarget::CreateDatabase { name, .. } => vec![dbn(Some(name.clone()), Action::Create)],
        DdlTarget::DropDatabase { name, .. } => vec![dbn(Some(name.clone()), Action::Drop)],
        DdlTarget::AlterDatabase { name, .. } => vec![dbn(name.clone(), Action::Alter)],
        DdlTarget::CreateObject {
            kind,
            name,
            temporary,
            parent,
            like,
            or_replace,
            ..
        } => {
            let mut v = Vec::new();
            match kind {
                DdlKind::Table | DdlKind::Sequence => {
                    let a = if *temporary {
                        Action::CreateTemporaryTables
                    } else {
                        Action::Create
                    };
                    v.push(dbo(name, a));
                    if let Some(l) = like {
                        v.push(tbl(l, &[Action::Select]));
                    }
                }
                DdlKind::View => {
                    v.push(dbo(name, Action::CreateView));
                    if *or_replace {
                        v.push(dbo(name, Action::Drop));
                    }
                }
                DdlKind::Procedure | DdlKind::Function => {
                    v.push(dbn(db_of(name), Action::CreateRoutine));
                }
                DdlKind::Trigger => {
                    if let Some(p) = parent {
                        v.push(tbl(p, &[Action::Trigger]));
                    }
                }
                DdlKind::Event => v.push(dbn(db_of(name), Action::Event)),
            }
            v
        }
        DdlTarget::AlterObject { kind, name, op, .. } => match kind {
            DdlKind::Procedure | DdlKind::Function => vec![Need::Routine {
                db: db_of(name),
                name: name.name.clone(),
                kind: routine(*kind),
                action: Action::AlterRoutine,
            }],
            DdlKind::Event => vec![dbn(db_of(name), Action::Event)],
            DdlKind::Trigger => Vec::new(),
            _ => match op {
                AlterOp::CreateIndex { .. } | AlterOp::DropIndex { .. } => {
                    vec![tbl(name, &[Action::Index])]
                }
                AlterOp::Change {
                    rename_to: Some(to),
                } => vec![
                    tbl(name, &[Action::Alter]),
                    tbl(name, &[Action::Drop]),
                    dbo(to, Action::Create),
                    dbo(to, Action::Insert),
                ],
                AlterOp::Change { rename_to: None } => vec![tbl(name, &[Action::Alter])],
            },
        },
        DdlTarget::DropObjects {
            kind,
            names,
            temporary,
            ..
        } => {
            if *temporary {
                return Vec::new();
            }
            names
                .iter()
                .map(|n| match kind {
                    DdlKind::Procedure | DdlKind::Function => Need::Routine {
                        db: db_of(n),
                        name: n.name.clone(),
                        kind: routine(*kind),
                        action: Action::AlterRoutine,
                    },
                    DdlKind::Event => dbn(db_of(n), Action::Event),
                    // DROP TRIGGER needs TRIGGER on its table, which only
                    // MariaDB knows here.
                    DdlKind::Trigger => dbn(db_of(n), Action::Trigger),
                    _ => tbl(n, &[Action::Drop]),
                })
                .collect()
        }
        DdlTarget::RenameTables { pairs, .. } => pairs
            .iter()
            .flat_map(|(a, b)| {
                [
                    tbl(a, &[Action::Alter]),
                    tbl(a, &[Action::Drop]),
                    dbo(b, Action::Create),
                    dbo(b, Action::Insert),
                ]
            })
            .collect(),
        DdlTarget::Truncate { name } => vec![tbl(name, &[Action::Drop])],
    }
}

/// The parse view of a statement's tokens (chapter 07's AST path): the
/// same characters on the same lines, comments and executable-comment
/// markers blanked, string contents as `x`, `ANSI_QUOTES` identifiers
/// backquoted. `None` for text the view can't keep faithful.
fn parse_view(text: &str, toks: &[Token]) -> Option<String> {
    let bytes = text.as_bytes();
    let first = toks.first()?.start as usize;
    let last = toks.last()?.end as usize;
    let mut v = String::with_capacity(last - first);
    let blank = |v: &mut String, s: &str| {
        for ch in s.chars() {
            v.push(if ch == '\n' { '\n' } else { ' ' });
        }
    };
    let xs = |v: &mut String, s: &str| {
        for ch in s.chars() {
            v.push(if ch == '\n' { '\n' } else { 'x' });
        }
    };
    for (k, t) in toks.iter().enumerate() {
        let s = &text[t.range()];
        match t.kind {
            k2 if k2.is_trivia() => blank(&mut v, s),
            TokenKind::String => {
                v.push('\'');
                xs(&mut v, &s[1..s.len() - 1]);
                v.push('\'');
            }
            TokenKind::NationalString => {
                v.push_str(&s[..1]);
                v.push('\'');
                xs(&mut v, &s[2..s.len() - 1]);
                v.push('\'');
            }
            TokenKind::QuotedIdent => {
                if s.starts_with('"') {
                    let inner = &s[1..s.len() - 1];
                    if inner.contains('`') {
                        return None;
                    }
                    v.push('`');
                    v.push_str(inner);
                    v.push('`');
                } else {
                    v.push_str(s);
                }
            }
            TokenKind::Word if bytes[t.start as usize].is_ascii_digit() => return None,
            TokenKind::UserVar if s.contains(['\'', '"', '`', '.']) => return None,
            TokenKind::SysVar if s.contains('`') => return None,
            TokenKind::Op
                if s == "-"
                    && toks.get(k + 1).is_some_and(|n| {
                        n.start == t.end && n.kind == TokenKind::Op && &text[n.range()] == "-"
                    }) =>
            {
                return None;
            }
            TokenKind::Unterminated | TokenKind::Other => return None,
            _ => v.push_str(s),
        }
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_proto_mysql::Persona;

    fn reqs(s: &str) -> Vec<Need> {
        requirements(s.as_bytes(), &LexOptions::for_persona(Persona::MariaDb))
            .unwrap_or_else(|| panic!("not modelled: {s}"))
    }

    fn t(db: Option<&str>, name: &str, a: &[Action]) -> Need {
        Need::Table {
            db: db.map(str::to_owned),
            name: name.to_owned(),
            any: ActionSet::of(a),
        }
    }

    #[test]
    fn selects_read_every_relation_but_ctes() {
        assert_eq!(
            reqs(
                "WITH c AS (SELECT * FROM shop.orders) SELECT * FROM c JOIN items i ON i.id = c.id"
            ),
            vec![
                t(Some("shop"), "orders", &[Action::Select]),
                t(None, "items", &[Action::Select])
            ]
        );
        assert_eq!(
            reqs("SELECT (SELECT max(x) FROM `crm`.`b`) FROM a /* c */ WHERE y = 'z'"),
            vec![
                t(Some("crm"), "b", &[Action::Select]),
                t(None, "a", &[Action::Select])
            ]
        );
    }

    #[test]
    fn writes_name_their_targets() {
        assert_eq!(
            reqs("INSERT INTO shop.orders (a) SELECT a FROM shop.cart"),
            vec![
                t(Some("shop"), "orders", &[Action::Insert]),
                t(Some("shop"), "cart", &[Action::Select])
            ]
        );
        assert_eq!(
            reqs("REPLACE INTO t VALUES (1)"),
            vec![
                t(None, "t", &[Action::Insert]),
                t(None, "t", &[Action::Delete])
            ]
        );
        assert_eq!(
            reqs("INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = 2"),
            vec![
                t(None, "t", &[Action::Insert]),
                t(None, "t", &[Action::Update])
            ]
        );
        assert_eq!(
            reqs("UPDATE t SET a = (SELECT 1 FROM u) WHERE b = 2"),
            vec![
                t(None, "t", &[Action::Update]),
                t(None, "u", &[Action::Select])
            ]
        );
        assert_eq!(
            reqs("UPDATE t JOIN u ON u.id = t.id SET t.a = u.b"),
            vec![
                t(None, "t", &[Action::Update, Action::Select]),
                t(None, "u", &[Action::Update, Action::Select])
            ]
        );
        assert_eq!(
            reqs("DELETE FROM shop.orders WHERE id IN (SELECT id FROM shop.old)"),
            vec![
                t(Some("shop"), "orders", &[Action::Delete]),
                t(Some("shop"), "old", &[Action::Select])
            ]
        );
        assert_eq!(
            reqs("DELETE o FROM orders o JOIN items i ON i.o = o.id"),
            vec![
                t(None, "orders", &[Action::Delete]),
                t(None, "items", &[Action::Select])
            ]
        );
    }

    #[test]
    fn unmodelled_text_is_none() {
        let o = LexOptions::for_persona(Persona::MariaDb);
        assert!(requirements(b"SHOW TABLES", &o).is_none());
        assert!(requirements(b"SELECT 1; SELECT 2", &o).is_none());
    }
}
