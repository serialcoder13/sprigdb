//! Inserts into parent members (chapter 15, "Table groups" and "Filling
//! and back-filling `_sprig_rk`"; chapter 17, "INSERT routing").
//!
//! A parent member (`order_items`) has no key column: its rows go where
//! their parent row (`orders`) is, through the declared foreign key, and
//! carry the parent's routing hash in `_sprig_rk`.
//!
//! - **One shard:** the statement goes to it unchanged; once the member
//!   fills `_sprig_rk`, each row gets `(SELECT _sprig_rk FROM parent WHERE
//!   … LIMIT 1)`, so MariaDB reads the parent in the same statement, and
//!   its own foreign-key check answers for a missing parent (1452, or a
//!   warning under `IGNORE`).
//! - **Several shards:** [`Plan::ViaParent`]. The executor reads where
//!   each distinct parent key is (through the parent's public unique
//!   lookup when it has one, else one `UNION ALL` statement per shard),
//!   then [`ViaParent::finish`] sends each row to its parent's shard.
//!   Rows whose parent is nowhere go to the first shard without a hash,
//!   so MariaDB's foreign-key check answers for them exactly as an
//!   unsharded table would; with `foreign_key_checks = 0` they are
//!   refused (chapter 19's orphanage isn't built).
//!
//! The foreign-key check on the shard is what makes this safe: a child row
//! can only commit next to its parent, so a parent that moved or vanished
//! between the read and the write fails the statement (a fence error or
//! 1452) rather than misplacing the row.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, Insert, ObjectName, ObjectNamePart, SetExpr,
    Statement, UnaryOperator, Value, visit_relations_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::{PlanCtx, R, Src, assigns_key, ident_parts, loc_of, one_shard_insert, shard_insert};
use crate::plan::{ParentHits, Plan, ViaParent, WriteKind, WritePlan};
use crate::reject::{Reason, Rejection, reject};
use crate::topology::{ParentLink, TableEntry};

const RK: &str = sprig_shard::rk::RK_COLUMN;

/// Plans an `INSERT` into the parent member `entry`, linked by `link`.
pub(super) fn plan_insert(
    stmt: &Statement,
    text_src: Option<&Src<'_>>,
    entry: &TableEntry,
    link: &ParentLink,
    default_schema: &str,
    cx: &PlanCtx<'_>,
) -> R<Plan> {
    let Statement::Insert(ins) = stmt else {
        return reject(Reason::Unsupported, "not an INSERT");
    };
    let sqlparser::ast::TableObject::TableName(name) = &ins.table else {
        return reject(Reason::Unsupported, "INSERT INTO a table function");
    };
    let loc = loc_of(name);
    if (ins.on.is_some() || ins.replace_into) && !link.upsert_safe {
        return reject(
            Reason::UpsertUnique,
            "an upsert on a table whose unique keys lack the foreign key its rows follow",
        );
    }
    if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) = &ins.on
        && assignments
            .iter()
            .any(|a| link.columns.iter().any(|c| assigns_key(&a.target, c)))
    {
        return reject(
            Reason::KeyChange,
            "ON DUPLICATE KEY UPDATE of the foreign key a parent member follows",
        );
    }
    super::lookups::check_gap(entry)?;
    if !entry.lookups.is_empty() {
        return reject(
            Reason::Lookup,
            "an INSERT into a parent member with a unique lookup",
        );
    }
    let one_shard = entry.group.shards.len() == 1;
    // Each row's foreign-key values.
    let keys: Vec<Vec<&Expr>> = match &ins.source {
        None => {
            // INSERT … SET: one row.
            let mut row = Vec::new();
            for c in &link.columns {
                let Some(a) = ins.assignments.iter().find(|a| assigns_key(&a.target, c)) else {
                    return reject(
                        Reason::InsertKey,
                        format!("an INSERT without {c}, the foreign key its rows follow"),
                    );
                };
                row.push(&a.value);
            }
            vec![row]
        }
        Some(src) => {
            let SetExpr::Values(values) = &*src.body else {
                return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
            };
            let width = if ins.columns.is_empty() {
                entry.columns.len()
            } else {
                ins.columns.len()
            };
            if width > 0 && values.rows.iter().any(|r| r.len() != width) {
                // A count mismatch: MariaDB's own 1136 says so, from any
                // shard.
                return Ok(Plan::Pass(one_shard_insert(
                    stmt,
                    text_src,
                    entry,
                    0,
                    loc,
                    default_schema,
                )));
            }
            let mut at = Vec::new();
            for (i, c) in link.columns.iter().enumerate() {
                let p = if ins.columns.is_empty() {
                    link.ordinals.get(i).copied().flatten()
                } else {
                    ins.columns.iter().position(|n| named(n, c))
                };
                let Some(p) = p else {
                    return reject(
                        Reason::InsertKey,
                        format!("an INSERT without {c}, the foreign key its rows follow"),
                    );
                };
                at.push(p);
            }
            let mut rows = Vec::with_capacity(values.rows.len());
            for r in &values.rows {
                let mut row = Vec::with_capacity(at.len());
                for p in &at {
                    let Some(e) = r.get(*p) else {
                        return reject(Reason::InsertKey, "a row without its foreign key");
                    };
                    row.push(e);
                }
                rows.push(row);
            }
            rows
        }
    };
    // Each distinct parent key, as a condition on the parent.
    let mut probes: Vec<String> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut row_probe = Vec::with_capacity(keys.len());
    let mut first_of: Vec<Vec<&Expr>> = Vec::new();
    for row in &keys {
        let mut conds = Vec::with_capacity(row.len());
        for (e, pc) in row.iter().zip(&link.parent_columns) {
            conds.push(format!("{} = {}", quote(pc), literal(e)?));
        }
        let cond = conds.join(" AND ");
        let next = probes.len();
        let i = *index.entry(cond.clone()).or_insert(next);
        if i == next {
            probes.push(cond);
            first_of.push(row.clone());
        }
        row_probe.push(i);
    }
    if one_shard {
        if !entry.fill_rk || names_rk(ins) {
            return Ok(Plan::Pass(one_shard_insert(
                stmt,
                text_src,
                entry,
                0,
                loc,
                default_schema,
            )));
        }
        let schema = entry.group.shards[0].schema.clone();
        let rks: Vec<Expr> = row_probe
            .iter()
            .map(|&p| rk_subquery(&schema, &link.table, &probes[p]))
            .collect::<R<_>>()?;
        let filled = with_rk(ins, entry, &rks)?;
        return Ok(Plan::Pass(one_shard_insert(
            &filled,
            None,
            entry,
            0,
            loc,
            default_schema,
        )));
    }
    if ins.returning.is_some() {
        return reject(
            Reason::Unsupported,
            "INSERT … RETURNING into a parent member over several shards",
        );
    }
    // The statement finish() splits: with the column list written out
    // when `_sprig_rk` will be added.
    let mut planned = ins.clone();
    let fill = entry.fill_rk && !names_rk(ins);
    if fill && ins.source.is_some() && ins.columns.is_empty() {
        if entry.columns.is_empty() {
            return reject(
                Reason::InsertKey,
                "an INSERT without a column list into a table whose columns are unknown",
            );
        }
        planned.columns = entry.columns.iter().map(|c| column(c)).collect();
    }
    let lookup = link.lookup.as_ref().and_then(|l| {
        let part = l.index.columns.first()?.class;
        let values = first_of
            .iter()
            .map(|row| match row.as_slice() {
                [e] => super::lookups::value_of(e, l.class, part),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some((Arc::clone(l), values))
    });
    Ok(Plan::ViaParent(Box::new(ViaParent {
        stmt: Statement::Insert(planned),
        table: entry.name.clone(),
        default_schema: default_schema.to_owned(),
        parent: link.table.clone(),
        group: Arc::clone(&entry.group),
        probes,
        lookup,
        row_probe,
        fill_rk: fill,
        batch: cx.lookup_batch.max(1),
    })))
}

impl ViaParent {
    /// The plan once the parents were found: `hits[p]` is where probe `p`'s
    /// parent row is; `fk_checks` is the session's `foreign_key_checks`.
    ///
    /// # Errors
    /// 39420 (`insert_key`) for a parent key held on several shards, a
    /// parent row without its routing hash, or a missing parent with
    /// `foreign_key_checks = 0`.
    pub fn finish(&self, hits: &[ParentHits], fk_checks: bool) -> Result<Plan, Rejection> {
        let Statement::Insert(ins) = &self.stmt else {
            return reject(Reason::Unsupported, "not an INSERT");
        };
        let sqlparser::ast::TableObject::TableName(name) = &ins.table else {
            return reject(Reason::Unsupported, "INSERT INTO a table function");
        };
        let loc = loc_of(name);
        // Per row: its shard and its hash.
        let mut placed: Vec<(usize, Option<u64>)> = Vec::with_capacity(self.row_probe.len());
        for &p in &self.row_probe {
            let mut at: Option<(usize, Option<u64>)> = None;
            for &(s, rk) in hits.get(p).map_or(&[][..], Vec::as_slice) {
                match at {
                    None => at = Some((s, rk)),
                    Some((s0, _)) if s0 != s => {
                        return reject(
                            Reason::InsertKey,
                            "a parent key held by rows on several shards",
                        );
                    }
                    Some(_) => {}
                }
            }
            match at {
                Some((s, rk)) => {
                    if self.fill_rk && rk.is_none() {
                        return reject(
                            Reason::InsertKey,
                            "a parent row without its routing hash (_sprig_rk)",
                        );
                    }
                    placed.push((s, rk));
                }
                // MariaDB's foreign-key check answers for the row there.
                None if fk_checks => placed.push((0, None)),
                None => {
                    return reject(
                        Reason::InsertKey,
                        "a row whose parent doesn't exist, with foreign_key_checks = 0 \
                         (chapter 19's orphanage isn't built)",
                    );
                }
            }
        }
        let mut by_shard: Vec<(usize, Vec<usize>)> = Vec::new();
        for (r, (s, _)) in placed.iter().enumerate() {
            match by_shard.iter_mut().find(|(x, _)| x == s) {
                Some((_, rows)) => rows.push(r),
                None => by_shard.push((*s, vec![r])),
            }
        }
        let number = |h: Option<u64>| match h {
            Some(h) => Expr::value(Value::Number(h.to_string(), false)),
            None => Expr::value(Value::Null),
        };
        let mut targets = Vec::with_capacity(by_shard.len());
        for (shard, rows) in &by_shard {
            let mut out = ins.clone();
            match &mut out.source {
                Some(src) => {
                    let SetExpr::Values(values) = &mut *src.body else {
                        return reject(Reason::InsertSelect, "INSERT … SELECT");
                    };
                    let all = std::mem::take(&mut values.rows);
                    values.rows = rows
                        .iter()
                        .filter_map(|&r| {
                            let mut row = all.get(r)?.clone();
                            if self.fill_rk {
                                row.push(number(placed[r].1));
                            }
                            Some(row)
                        })
                        .collect();
                    if self.fill_rk {
                        out.columns.push(column(RK));
                    }
                }
                None if self.fill_rk => out.assignments.push(Assignment {
                    target: AssignmentTarget::ColumnName(column(RK)),
                    value: number(placed.first().and_then(|p| p.1)),
                }),
                None => {}
            }
            targets.push(shard_insert(
                &Statement::Insert(out),
                None,
                &self.group,
                &self.table,
                *shard,
                loc,
                &self.default_schema,
            ));
        }
        if targets.len() == 1 {
            return Ok(Plan::Pass(targets.remove(0)));
        }
        Ok(Plan::Write(WritePlan {
            kind: WriteKind::Insert,
            targets,
            rows: self.row_probe.len() as u64,
            claims: Vec::new(),
            releases: Vec::new(),
            fk: None,
        }))
    }
}

fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn column(n: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new(n))])
}

fn named(c: &ObjectName, n: &str) -> bool {
    ident_parts(c)
        .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(n)))
        .unwrap_or(false)
}

/// Whether the statement writes `_sprig_rk` itself (left as written).
fn names_rk(ins: &Insert) -> bool {
    ins.columns.iter().any(|c| named(c, RK))
        || ins.assignments.iter().any(|a| assigns_key(&a.target, RK))
}

/// A foreign-key value as SQL text: a literal only (a number, a string, a
/// hex or charset-introduced string, a signed number). `NULL` names no
/// parent, so the row has no shard.
fn literal(e: &Expr) -> R<String> {
    let ok = match e {
        Expr::Value(v) => match &v.value {
            Value::Null => {
                return reject(
                    Reason::InsertKey,
                    "a NULL foreign key in a table whose rows follow their parent",
                );
            }
            Value::Placeholder(_) => false,
            _ => true,
        },
        Expr::Prefixed { value, .. } => matches!(&**value, Expr::Value(_)),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => matches!(&**expr, Expr::Value(v) if matches!(v.value, Value::Number(..))),
        Expr::Nested(inner) => return literal(inner),
        _ => false,
    };
    if ok {
        Ok(e.to_string())
    } else {
        reject(
            Reason::InsertKey,
            format!("a foreign key that isn't a literal: {e}"),
        )
    }
}

/// `(SELECT _sprig_rk FROM schema.parent WHERE cond LIMIT 1)`, its names
/// without parser positions (the renderer moves names by position).
fn rk_subquery(schema: &str, parent: &str, cond: &str) -> R<Expr> {
    let text = format!(
        "(SELECT {} FROM {}.{} WHERE {cond} LIMIT 1)",
        quote(RK),
        quote(schema),
        quote(parent)
    );
    let mut e = Parser::new(&MySqlDialect {})
        .try_with_sql(&text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|_| Rejection::new(Reason::InsertKey, "a parent lookup"))?;
    let _ = visit_relations_mut(&mut e, |n| {
        let parts: Vec<ObjectNamePart> = ident_parts(n)
            .unwrap_or_default()
            .into_iter()
            .map(|i| ObjectNamePart::Identifier(Ident::with_quote('`', i.value.clone())))
            .collect();
        *n = ObjectName(parts);
        ControlFlow::<()>::Continue(())
    });
    Ok(e)
}

/// `ins` with `_sprig_rk` set to `rks[r]` in each row `r` (the column list
/// written out first when the statement has none).
fn with_rk(ins: &Insert, entry: &TableEntry, rks: &[Expr]) -> R<Statement> {
    let mut out = ins.clone();
    match &mut out.source {
        None => {
            let Some(v) = rks.first() else {
                return reject(Reason::InsertKey, "a row");
            };
            out.assignments.push(Assignment {
                target: AssignmentTarget::ColumnName(column(RK)),
                value: v.clone(),
            });
        }
        Some(src) => {
            let SetExpr::Values(values) = &mut *src.body else {
                return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
            };
            if out.columns.is_empty() {
                if entry.columns.is_empty() {
                    return reject(
                        Reason::InsertKey,
                        "an INSERT without a column list into a table whose columns are unknown",
                    );
                }
                out.columns = entry.columns.iter().map(|c| column(c)).collect();
            }
            out.columns.push(column(RK));
            for (row, v) in values.rows.iter_mut().zip(rks) {
                row.push(v.clone());
            }
        }
    }
    Ok(Statement::Insert(out))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use sprig_common::Result;
    use sprig_common::ids::{ClusterId, GroupId, TenantId};
    use sprig_shard::{KeySpec, ShardMap};

    use crate::exec::{Budget, Output, ShardBackend, ShardOk, ShardOutcome, ShardRows, execute};
    use crate::plan::{Plan, Target};
    use crate::planner::{PlanCtx, plan};
    use crate::reject::Reason;
    use crate::topology::catalog::{CatalogGroup, CatalogTable, Membership, TableShape, build};
    use crate::topology::{KeyValue, StaticTopology, Topology};

    const DEF: &str = "t7_d1_g1_s1";
    const INVOICES: &str = "CREATE TABLE `invoices` (\n  `iid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  `total` decimal(10,2) DEFAULT NULL,\n  PRIMARY KEY (`iid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    const LINES: &str = "CREATE TABLE `invoice_lines` (\n  `iid` int(11) NOT NULL,\n  `line_no` int(11) NOT NULL,\n  `sku` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`iid`,`line_no`),\n  KEY `fk_il_inv` (`iid`),\n  CONSTRAINT `fk_il_inv` FOREIGN KEY (`iid`) REFERENCES `invoices` (`iid`) ON DELETE CASCADE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    fn topo(shards: u32, fill_rk: bool, parent_group: u32) -> StaticTopology {
        let c1 = ClusterId::new("c1").unwrap();
        let c2 = ClusterId::new("c2").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let group = |id: u32| CatalogGroup {
            id,
            spec: spec.clone(),
            map: Arc::new(
                ShardMap::presplit(
                    TenantId(7),
                    GroupId(id),
                    spec.digest,
                    shards,
                    &[c1.clone(), c2.clone()],
                )
                .unwrap(),
            ),
        };
        let table =
            |name: &str, group: u32, key: Option<&str>, sql: &str, fk: Option<&str>| CatalogTable {
                object: 0,
                db: 1,
                default_schema: DEF.into(),
                name: name.into(),
                member: Membership::Sharded {
                    group,
                    key_columns: key.map(|k| vec![k.to_owned()]),
                    create_sql: Some(sql.into()),
                    lookups: Vec::new(),
                    fill_rk,
                    parent_fk: fk.map(str::to_owned),
                },
            };
        build(
            7,
            &[group(3), group(4)],
            &[
                table("invoices", parent_group, Some("cid"), INVOICES, None),
                table("invoice_lines", 3, None, LINES, Some("fk_il_inv")),
            ],
        )
    }

    fn p_with(t: &StaticTopology, sql: &str) -> Plan {
        let cx = PlanCtx {
            topo: t,
            default_schema: Some(DEF),
            session_cluster: "c1",
            max_route_values: 1024,
            lookup_batch: 2,
            ids: crate::planner::IdMode::default(),
        };
        plan(sql, &cx)
    }

    fn rejected(t: &StaticTopology, sql: &str) -> Reason {
        match p_with(t, sql) {
            Plan::Reject(r) => r.reason,
            other => panic!("{sql}: expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn foreign_keys_are_read_from_show_create_table() {
        let s = TableShape::parse(LINES);
        let [f] = s.foreign_keys.as_slice() else {
            panic!("{:?}", s.foreign_keys)
        };
        assert_eq!(f.name, "fk_il_inv");
        assert_eq!(f.columns, ["iid"]);
        assert_eq!(f.ref_table, "invoices");
        assert_eq!(f.ref_columns, ["iid"]);
        let q = TableShape::parse(
            "CREATE TABLE `x` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL,\n  CONSTRAINT `two` FOREIGN KEY (`a`, `b`) REFERENCES `other_db`.`p` (`pa`, `pb`)\n) ENGINE=InnoDB",
        );
        assert_eq!(q.foreign_keys[0].ref_table, "p");
        assert_eq!(q.foreign_keys[0].columns, ["a", "b"]);
        assert_eq!(q.foreign_keys[0].ref_columns, ["pa", "pb"]);
    }

    #[test]
    fn the_link_holds_only_inside_one_group() {
        let t = topo(4, false, 3);
        let e = t.table(DEF, "invoice_lines").unwrap();
        let l = e.parent.as_ref().unwrap();
        assert_eq!(l.table, "invoices");
        assert_eq!(l.columns, ["iid"]);
        assert_eq!(l.ordinals, [Some(0)]);
        assert!(l.upsert_safe);
        // A parent in another group: no link, inserts refused as before.
        let t = topo(4, false, 4);
        assert!(t.table(DEF, "invoice_lines").unwrap().parent.is_none());
        assert_eq!(
            rejected(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')"),
            Reason::InsertKey
        );
    }

    #[test]
    fn one_shard_passes_and_fills_rk_from_the_parent_in_the_statement() {
        let t = topo(1, false, 3);
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')") else {
            panic!()
        };
        assert_eq!(
            x.sql,
            "INSERT INTO `t7_d1_g3_s1`.invoice_lines VALUES (1, 1, 'a')"
        );
        let t = topo(1, true, 3);
        let Plan::Pass(x) = p_with(
            &t,
            "INSERT IGNORE INTO invoice_lines (line_no, iid) VALUES (1, 7), (2, '8')",
        ) else {
            panic!()
        };
        assert!(
            x.sql.contains(
                "(line_no, iid, _sprig_rk) VALUES (1, 7, (SELECT `_sprig_rk` FROM `t7_d1_g3_s1`.`invoices` WHERE `iid` = 7 LIMIT 1))"
            ),
            "{}",
            x.sql
        );
        assert!(x.sql.contains("WHERE `iid` = '8' LIMIT 1))"), "{}", x.sql);
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')") else {
            panic!()
        };
        assert!(
            x.sql.contains("(iid, line_no, sku, _sprig_rk)"),
            "{}",
            x.sql
        );
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines SET iid = 3, line_no = 1") else {
            panic!()
        };
        assert!(x.sql.contains("_sprig_rk = (SELECT"), "{}", x.sql);
    }

    #[test]
    fn refusals() {
        let t = topo(4, true, 3);
        for (sql, r) in [
            (
                "INSERT INTO invoice_lines VALUES (NULL, 1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1 + 1, 1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines (line_no, sku) VALUES (1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines SELECT * FROM invoice_lines",
                Reason::InsertSelect,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') ON DUPLICATE KEY UPDATE iid = 2",
                Reason::KeyChange,
            ),
            (
                "UPDATE invoice_lines SET iid = 2 WHERE line_no = 1",
                Reason::KeyChange,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') RETURNING sku",
                Reason::Unsupported,
            ),
        ] {
            assert_eq!(rejected(&t, sql), r, "{sql}");
        }
        // Upserts are exact: every unique key holds the foreign key.
        assert!(matches!(
            p_with(
                &t,
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') ON DUPLICATE KEY UPDATE sku = 'b'"
            ),
            Plan::ViaParent(_)
        ));
        // One shard, no _sprig_rk yet: changing the key is harmless.
        let t1 = topo(1, false, 3);
        assert!(matches!(
            p_with(&t1, "UPDATE invoice_lines SET iid = 2 WHERE line_no = 1"),
            Plan::Pass(_)
        ));
    }

    #[test]
    fn a_join_to_the_parent_on_the_foreign_key_is_co_located() {
        let t = topo(4, true, 3);
        let x = match p_with(
            &t,
            "SELECT i.iid, l.line_no FROM invoices i JOIN invoice_lines l ON l.iid = i.iid WHERE i.cid = 42",
        ) {
            Plan::Pass(x) => x,
            other => panic!("{other:?}"),
        };
        assert!(
            x.sql.contains(&format!("`t7_d1_g3_s{}`.invoices", x.shard))
                && x.sql
                    .contains(&format!("`t7_d1_g3_s{}`.invoice_lines", x.shard)),
            "{}",
            x.sql
        );
        match p_with(
            &t,
            "SELECT i.cid, COUNT(*) FROM invoice_lines l LEFT JOIN invoices i ON i.iid = l.iid GROUP BY i.cid",
        ) {
            Plan::Read(r) => assert_eq!(r.targets.len(), 4),
            other => panic!("{other:?}"),
        }
        // Another column is no parent edge: not planned as co-located.
        assert!(matches!(
            p_with(
                &t,
                "SELECT * FROM invoices i JOIN invoice_lines l ON l.line_no = i.iid"
            ),
            Plan::Reject(_) | Plan::LookupJoin(_) | Plan::Gather(_)
        ));
    }

    /// Answers parent lookups from `parents` (iid → cid) and records the
    /// rest.
    struct Fake {
        t: StaticTopology,
        parents: HashMap<i64, i64>,
        fk_checks: bool,
        log: Vec<(u32, String)>,
    }

    #[async_trait::async_trait]
    impl ShardBackend for Fake {
        async fn run(
            &mut self,
            target: &Target,
            sql: &str,
            _budget: &mut Budget,
        ) -> Result<ShardOutcome> {
            self.log.push((target.shard, sql.to_owned()));
            if !sql.starts_with("SELECT ") {
                return Ok(ShardOutcome::Ok(ShardOk {
                    affected: 1,
                    ..ShardOk::default()
                }));
            }
            let g = &self.t.table(DEF, "invoices").unwrap().group;
            let mut rows = Vec::new();
            for part in sql.split(" UNION ALL ") {
                let b = |s: String| Some(bytes::Bytes::from(s));
                if part.contains("@@foreign_key_checks") {
                    let f = if self.fk_checks { "1" } else { "0" };
                    rows.push(vec![b("-1".into()), b(f.into())]);
                    continue;
                }
                let words: Vec<&str> = part.split_whitespace().collect();
                let probe = words[1].trim_end_matches(',');
                let iid: i64 = words.last().unwrap().trim_matches('\'').parse().unwrap();
                let Some(cid) = self.parents.get(&iid) else {
                    continue;
                };
                let i = g.shard_of(&KeyValue::Int(i128::from(*cid))).unwrap();
                if g.shards[i].shard != target.shard {
                    continue;
                }
                let h = g.hash_of(&KeyValue::Int(i128::from(*cid))).unwrap();
                rows.push(vec![b(probe.into()), b(h.to_string())]);
            }
            Ok(ShardOutcome::Rows(ShardRows {
                columns: Vec::new(),
                rows,
                warnings: 0,
            }))
        }
        fn in_transaction(&self) -> bool {
            false
        }
    }

    async fn run(fk_checks: bool, sql: &str) -> (Result<Output>, Vec<(u32, String)>, Fake) {
        let t = topo(4, true, 3);
        let plan = p_with(&t, sql);
        assert!(matches!(plan, Plan::ViaParent(_)), "{plan:?}");
        let mut f = Fake {
            t: topo(4, true, 3),
            parents: [(10, 1), (11, 2), (12, 3), (13, 4), (14, 5)]
                .into_iter()
                .collect(),
            fk_checks,
            log: Vec::new(),
        };
        let mut budget = Budget::mb(16);
        let out = execute(&plan, &mut f, &mut budget).await;
        let log = std::mem::take(&mut f.log);
        (out, log, f)
    }

    #[tokio::test]
    async fn rows_follow_their_parents_to_their_shards() {
        let (out, log, f) = run(
            true,
            "INSERT INTO invoice_lines VALUES (10, 1, 'a'), (11, 1, 'b'), (12, 1, 'c'), (10, 2, 'd'), (13, 1, 'e'), (14, 1, 'f')",
        )
        .await;
        out.unwrap();
        let g = &f.t.table(DEF, "invoices").unwrap().group;
        // Five distinct parents, batches of two: three statements a shard.
        let reads = log.iter().filter(|(_, s)| s.starts_with("SELECT")).count();
        assert_eq!(reads, 12, "{log:#?}");
        let writes: Vec<&(u32, String)> = log
            .iter()
            .filter(|(_, s)| s.starts_with("INSERT"))
            .collect();
        let mut seen = 0;
        for (iid, cid) in [(10, 1), (11, 2), (12, 3), (13, 4), (14, 5)] {
            let i = g.shard_of(&KeyValue::Int(cid)).unwrap();
            let h = g.hash_of(&KeyValue::Int(cid)).unwrap();
            let s = &g.shards[i];
            let (_, w) = writes
                .iter()
                .find(|(sh, w)| *sh == s.shard && w.contains(&format!("({iid}, ")))
                .unwrap_or_else(|| panic!("{iid} not on shard {}: {writes:#?}", s.shard));
            assert!(w.contains(&format!("`{}`.invoice_lines", s.schema)), "{w}");
            assert!(w.contains("_sprig_rk)"), "{w}");
            assert!(w.contains(&h.to_string()), "{w}");
            seen += 1;
        }
        assert_eq!(seen, 5);
        // Both lines of invoice 10 went in one statement, in order.
        let ten = writes.iter().find(|(_, w)| w.contains("(10, 1,")).unwrap();
        assert!(ten.1.find("(10, 1,").unwrap() < ten.1.find("(10, 2,").unwrap());
    }

    #[tokio::test]
    async fn a_missing_parent_is_left_to_mariadb_or_refused() {
        let (out, log, _) = run(true, "INSERT INTO invoice_lines VALUES (99, 1, 'x')").await;
        out.unwrap();
        let w = log.iter().find(|(_, s)| s.starts_with("INSERT")).unwrap();
        assert_eq!(w.0, 1, "the first shard answers with its own 1452");
        assert!(w.1.ends_with("(99, 1, 'x', NULL)"), "{}", w.1);
        let (out, log, _) = run(false, "INSERT INTO invoice_lines VALUES (99, 1, 'x')").await;
        let e = out.unwrap_err().to_string();
        assert!(
            e.contains("insert_key") && e.contains("foreign_key_checks"),
            "{e}"
        );
        assert!(!log.iter().any(|(_, s)| s.starts_with("INSERT")));
    }

    /// A parent with a public unique lookup on its referenced column is
    /// found through it: no statement per shard.
    #[tokio::test]
    async fn parents_are_found_through_their_unique_lookup() {
        use crate::plan::LookupProbe;
        use crate::topology::catalog::CatalogLookup;
        use sprig_shard::lookup::{LookupOrigin, LookupState};

        struct ByLookup {
            log: Vec<String>,
            owners: HashMap<i128, u64>,
        }
        #[async_trait::async_trait]
        impl ShardBackend for ByLookup {
            async fn run(
                &mut self,
                _t: &Target,
                sql: &str,
                _b: &mut Budget,
            ) -> Result<ShardOutcome> {
                self.log.push(sql.to_owned());
                Ok(ShardOutcome::Ok(ShardOk::default()))
            }
            fn in_transaction(&self) -> bool {
                false
            }
            async fn lookup_owners(&mut self, probe: &LookupProbe) -> Result<Vec<u64>> {
                let [v] = probe.values.as_slice() else {
                    panic!()
                };
                let [sprig_shard::KeyValueOwned::Int(i)] = v.as_slice() else {
                    panic!("{v:?}")
                };
                Ok(self.owners.get(i).copied().into_iter().collect())
            }
        }
        let c1 = ClusterId::new("c1").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let map = ShardMap::presplit(
            TenantId(7),
            GroupId(3),
            spec.digest,
            4,
            std::slice::from_ref(&c1),
        )
        .unwrap();
        let lmap = ShardMap::presplit(TenantId(7), GroupId(5), 0, 2, &[c1]).unwrap();
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let lookup = CatalogLookup {
            id: 11,
            owner: 70,
            index_name: "PRIMARY".into(),
            columns: vec!["iid".into()],
            unique: true,
            origin: LookupOrigin::PrimaryKey,
            state: LookupState::Public,
            map: Some(Arc::new(lmap)),
        };
        let t = build(
            7,
            &[g],
            &[
                CatalogTable {
                    object: 0,
                    db: 1,
                    default_schema: DEF.into(),
                    name: "invoices".into(),
                    member: Membership::Sharded {
                        group: 3,
                        key_columns: Some(vec!["cid".into()]),
                        create_sql: Some(INVOICES.into()),
                        lookups: vec![lookup],
                        fill_rk: true,
                        parent_fk: None,
                    },
                },
                CatalogTable {
                    object: 0,
                    db: 1,
                    default_schema: DEF.into(),
                    name: "invoice_lines".into(),
                    member: Membership::Sharded {
                        group: 3,
                        key_columns: None,
                        create_sql: Some(LINES.into()),
                        lookups: Vec::new(),
                        fill_rk: true,
                        parent_fk: Some("FK_IL_INV".into()),
                    },
                },
            ],
        );
        let e = t.table(DEF, "invoice_lines").unwrap();
        assert!(e.parent.as_ref().unwrap().lookup.is_some());
        let plan = p_with(
            &t,
            "INSERT INTO invoice_lines VALUES (10, 1, 'a'), (11, 1, 'b')",
        );
        let Plan::ViaParent(v) = &plan else {
            panic!("{plan:?}")
        };
        assert!(v.lookup.is_some());
        let h = |c: i128| e.group.hash_of(&KeyValue::Int(c)).unwrap();
        let mut be = ByLookup {
            log: Vec::new(),
            owners: [(10, h(1)), (11, h(1))].into_iter().collect(),
        };
        execute(&plan, &mut be, &mut Budget::mb(16)).await.unwrap();
        // Both parents hash alike: one statement, nothing read from shards.
        let [w] = be.log.as_slice() else {
            panic!("{:?}", be.log)
        };
        assert!(w.starts_with("INSERT"), "{w}");
        assert!(w.contains(&format!("(10, 1, 'a', {})", h(1))), "{w}");
    }

    #[tokio::test]
    async fn insert_set_follows_its_parent() {
        let (out, log, f) = run(true, "INSERT INTO invoice_lines SET iid = 12, line_no = 3").await;
        out.unwrap();
        let g = &f.t.table(DEF, "invoices").unwrap().group;
        let h = g.hash_of(&KeyValue::Int(3)).unwrap();
        let w = log.iter().find(|(_, s)| s.starts_with("INSERT")).unwrap();
        assert!(w.1.ends_with(&format!("_sprig_rk = {h}")), "{}", w.1);
    }
}
