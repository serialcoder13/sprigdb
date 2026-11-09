//! Foreign keys across groups, planned and run against a scripted backend.

#![allow(clippy::unwrap_used, clippy::panic)]

use std::collections::HashSet;
use std::sync::Arc;

use sprig_common::ids::{ClusterId, GroupId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_shard::fk::{FkEnforcement, FkState, ForeignKey};
use sprig_shard::{KeySpec, ShardMap};

use crate::exec::{Budget, ShardBackend, ShardOk, ShardOutcome, ShardRows, execute};
use crate::plan::{Plan, Target};
use crate::planner::{IdMode, PlanCtx, parse_template, plan, plan_prepared};
use crate::reject::Reason;
use crate::topology::StaticTopology;
use crate::topology::catalog::{CatalogGroup, CatalogTable, Membership, build_with_fks};

use super::FkAction;

const DEF: &str = "t7_d1_g1_s1";

const SUPPLIERS: &str = "CREATE TABLE `suppliers` (\n  `sid` int(11) NOT NULL,\n  `sname` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`sid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
const PARTS: &str = "CREATE TABLE `parts` (\n  `cid` int(11) NOT NULL,\n  `pno` int(11) NOT NULL,\n  `sid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`cid`,`pno`),\n  KEY `sid` (`sid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
const SHIPS: &str = "CREATE TABLE `ships` (\n  `cid` int(11) NOT NULL,\n  `shid` int(11) NOT NULL,\n  `sid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`cid`,`shid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
const CONTACTS: &str = "CREATE TABLE `contacts` (\n  `cid` int(11) NOT NULL,\n  `sid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`cid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

fn fk(id: u64, name: &str, child: u64, on_delete: FkAction) -> ForeignKey {
    ForeignKey {
        id,
        name: name.into(),
        child,
        child_columns: vec!["sid".into()],
        parent: 40,
        parent_columns: vec!["sid".into()],
        on_delete,
        on_update: FkAction::Restrict,
        enforcement: FkEnforcement::Gateway,
        child_lookup: None,
        state: FkState::Enforced,
    }
}

fn topo() -> StaticTopology {
    let c1 = ClusterId::new("c1").unwrap();
    let c2 = ClusterId::new("c2").unwrap();
    let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
    let g3 = ShardMap::presplit(
        TenantId(7),
        GroupId(3),
        spec.digest,
        4,
        &[c1.clone(), c2.clone()],
    )
    .unwrap();
    let g4 = ShardMap::presplit(TenantId(7), GroupId(4), spec.digest, 2, &[c2]).unwrap();
    let groups = [
        CatalogGroup {
            id: 3,
            spec: spec.clone(),
            map: Arc::new(g3),
        },
        CatalogGroup {
            id: 4,
            spec,
            map: Arc::new(g4),
        },
    ];
    let table = |object: u64, name: &str, group: u32, key: &str, sql: &str| CatalogTable {
        object,
        db: 1,
        default_schema: DEF.into(),
        name: name.into(),
        member: Membership::Sharded {
            group,
            key_columns: Some(vec![key.into()]),
            create_sql: Some(sql.into()),
            lookups: Vec::new(),
            fill_rk: false,
            parent_fk: None,
        },
    };
    let tables = [
        table(40, "suppliers", 4, "sid", SUPPLIERS),
        table(41, "parts", 3, "cid", PARTS),
        table(42, "ships", 3, "cid", SHIPS),
        table(43, "contacts", 3, "cid", CONTACTS),
    ];
    let fks = [
        fk(1, "fk_parts", 41, FkAction::Restrict),
        fk(2, "fk_ships", 42, FkAction::Cascade),
        fk(3, "fk_contacts", 43, FkAction::SetNull),
    ];
    build_with_fks(7, &groups, &tables, &fks)
}

fn p(t: &StaticTopology, sql: &str) -> Plan {
    let cx = PlanCtx {
        topo: t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode::default(),
    };
    plan(sql, &cx)
}

/// Answers probes for the supplier ids in `suppliers` (parent side) and
/// `children` (child side), and parent reads with `read`.
struct Script {
    log: Vec<(String, String)>,
    suppliers: HashSet<i64>,
    children: HashSet<i64>,
    read: Vec<i64>,
    checks: bool,
}

impl Script {
    fn new() -> Self {
        Self {
            log: Vec::new(),
            suppliers: HashSet::new(),
            children: HashSet::new(),
            read: Vec::new(),
            checks: true,
        }
    }
}

fn probe_hits(sql: &str, have: &HashSet<i64>) -> Vec<Vec<Option<bytes::Bytes>>> {
    sql.split(" UNION ALL ")
        .filter_map(|part| {
            let i: usize = part
                .strip_prefix("(SELECT ")?
                .split(' ')
                .next()?
                .parse()
                .ok()?;
            let v: i64 = part
                .split("`sid` = ")
                .nth(1)?
                .split(' ')
                .next()?
                .parse()
                .ok()?;
            have.contains(&v)
                .then(|| vec![Some(bytes::Bytes::from(i.to_string()))])
        })
        .collect()
}

#[async_trait::async_trait]
impl ShardBackend for Script {
    async fn run(&mut self, t: &Target, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        self.log.push((t.at.schema.clone(), sql.to_owned()));
        if sql.starts_with("(SELECT") {
            let have = if sql.contains("`suppliers`") {
                &self.suppliers
            } else {
                &self.children
            };
            return Ok(ShardOutcome::Rows(ShardRows {
                columns: vec![crate::exec::text_column("i")],
                rows: probe_hits(sql, have),
                warnings: 0,
            }));
        }
        if sql.starts_with("SELECT") && sql.ends_with("FOR UPDATE") {
            // The first shard answers the parent rows.
            let first = !self
                .log
                .iter()
                .rev()
                .skip(1)
                .any(|(_, s)| s.starts_with("SELECT") && s.ends_with("FOR UPDATE"));
            return Ok(ShardOutcome::Rows(ShardRows {
                columns: vec![crate::exec::text_column("sid")],
                rows: if first {
                    self.read
                        .iter()
                        .map(|v| vec![Some(bytes::Bytes::from(v.to_string()))])
                        .collect()
                } else {
                    Vec::new()
                },
                warnings: 0,
            }));
        }
        Ok(ShardOutcome::Ok(ShardOk::default()))
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn foreign_key_checks(&self) -> bool {
        self.checks
    }
}

async fn run(sql: &str, be: &mut Script) -> std::result::Result<(), SprigError> {
    let t = topo();
    let plan = p(&t, sql);
    execute(&plan, be, &mut Budget::mb(16)).await.map(|_| ())
}

fn sqls(be: &Script) -> Vec<&str> {
    be.log.iter().map(|(_, s)| s.as_str()).collect()
}

#[test]
fn a_child_insert_is_a_write_with_a_check() {
    let t = topo();
    let Plan::Write(w) = p(&t, "INSERT INTO parts VALUES (1, 1, 7), (1, 2, NULL)") else {
        panic!()
    };
    let f = w.fk.unwrap();
    assert_eq!(f.checks.len(), 1);
    assert_eq!(f.checks[0].tuples.len(), 1, "NULL needs no parent");
    // Only NULLs: nothing to check, relayed as before.
    assert!(matches!(
        p(&t, "INSERT INTO parts VALUES (1, 3, NULL)"),
        Plan::Pass(_)
    ));
    assert!(matches!(
        p(&t, "INSERT INTO parts (cid, pno) VALUES (1, 3)"),
        Plan::Pass(_)
    ));
}

#[test]
fn unchecked_forms_are_refused() {
    let t = topo();
    for sql in [
        "INSERT IGNORE INTO parts VALUES (1, 1, 7)",
        "INSERT INTO parts VALUES (1, 1, 3 + 4)",
        "INSERT INTO parts VALUES (1, 1, 7) ON DUPLICATE KEY UPDATE sid = 8",
        "UPDATE parts SET sid = sid + 1 WHERE cid = 1",
        "REPLACE INTO suppliers VALUES (7, 'x')",
        "DELETE parts FROM parts JOIN ships ON parts.cid = ships.cid WHERE parts.cid = 1",
    ] {
        match p(&t, sql) {
            Plan::Reject(r) => assert_eq!(r.reason, Reason::ForeignKey, "{sql}: {r:?}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_missing_parent_fails_before_the_rows() {
    let mut be = Script::new();
    let e = run("INSERT INTO parts VALUES (1, 1, 7)", &mut be)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1452, "{e}");
    assert!(e.message.contains("CONSTRAINT `fk_parts`"), "{e}");
    assert!(!sqls(&be).iter().any(|s| s.starts_with("INSERT")));
    assert_eq!(sqls(&be).last(), Some(&"ROLLBACK"));
    // The probe went to supplier 7's shard only, locking what it reads.
    let probes: Vec<&(String, String)> = be
        .log
        .iter()
        .filter(|(_, s)| s.starts_with("(SELECT"))
        .collect();
    assert_eq!(probes.len(), 1);
    assert!(probes[0].0.starts_with("t7_d1_g4_s"), "{probes:?}");
    assert!(probes[0].1.ends_with("LIMIT 1 LOCK IN SHARE MODE)"));

    let mut be = Script::new();
    be.suppliers.insert(7);
    run("INSERT INTO parts VALUES (1, 1, 7)", &mut be)
        .await
        .unwrap();
    assert!(sqls(&be).iter().any(|s| s.starts_with("INSERT")));
    assert_eq!(sqls(&be).last(), Some(&"COMMIT"));

    // foreign_key_checks = 0: no probe.
    let mut be = Script::new();
    be.checks = false;
    run("INSERT INTO parts VALUES (1, 1, 7)", &mut be)
        .await
        .unwrap();
    assert!(!sqls(&be).iter().any(|s| s.starts_with("(SELECT")));
}

#[tokio::test]
async fn an_update_of_the_child_key_checks_the_new_value() {
    let mut be = Script::new();
    be.suppliers.insert(8);
    run("UPDATE parts SET sid = 8 WHERE cid = 1", &mut be)
        .await
        .unwrap();
    let mut be = Script::new();
    let e = run("UPDATE parts SET sid = 9 WHERE cid = 1", &mut be)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1452);
}

#[tokio::test]
async fn a_referenced_parent_is_kept() {
    let mut be = Script::new();
    be.read = vec![7];
    be.children.insert(7);
    let e = run("DELETE FROM suppliers WHERE sid = 7", &mut be)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1451, "{e}");
    let s = sqls(&be);
    let read = s.iter().position(|x| x.ends_with("FOR UPDATE")).unwrap();
    let probe = s.iter().position(|x| x.starts_with("(SELECT")).unwrap();
    assert!(read < probe, "the parent is locked first: {s:?}");
    assert!(!s.iter().any(|x| x.starts_with("DELETE")));
}

#[tokio::test]
async fn cascades_and_set_null_run_before_the_parent_row() {
    let mut be = Script::new();
    be.read = vec![7];
    run("DELETE FROM suppliers WHERE sid = 7", &mut be)
        .await
        .unwrap();
    let s = sqls(&be);
    let ships: Vec<usize> = s
        .iter()
        .enumerate()
        .filter(|(_, x)| x.starts_with("DELETE FROM `t7_d1_g3_s") && x.contains("`ships`"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ships.len(), 4, "every ships shard: {s:?}");
    let nulls = s
        .iter()
        .filter(|x| x.starts_with("UPDATE") && x.contains("SET `sid` = NULL"))
        .count();
    assert_eq!(nulls, 4, "{s:?}");
    let parent = s
        .iter()
        .position(|x| x.starts_with("DELETE FROM `t7_d1_g4_s"))
        .unwrap();
    assert!(ships.iter().all(|&i| i < parent));
    assert_eq!(s.last(), Some(&"COMMIT"));

    // No parent row matched: nothing else runs.
    let mut be = Script::new();
    run("DELETE FROM suppliers WHERE sid = 70", &mut be)
        .await
        .unwrap();
    assert!(!sqls(&be).iter().any(|x| x.contains("`ships`")));
}

#[test]
fn prepared_child_inserts_render_their_values() {
    let t = topo();
    let cx = PlanCtx {
        topo: &t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode::default(),
    };
    let tpl = parse_template("INSERT INTO parts VALUES (?, ?, ?)", &t)
        .unwrap()
        .unwrap();
    let pp = plan_prepared(
        &tpl,
        &[
            crate::planner::ParamLit::Int(1),
            crate::planner::ParamLit::Int(1),
            crate::planner::ParamLit::Int(7),
        ],
        &cx,
    );
    let Plan::Write(w) = &pp.plan else {
        panic!("{:?}", pp.plan)
    };
    assert!(!pp.placeholders);
    assert!(!w.targets[0].sql.contains('?'), "{}", w.targets[0].sql);
    assert_eq!(w.fk.as_ref().unwrap().checks[0].tuples[0][0].sql, "7");
}

#[test]
fn limited_writes_are_a_locked_two_step_when_the_order_is_total() {
    let t = topo();
    // A child's own rows need no foreign-key work: deleting them in order
    // is a limited write too.
    let Plan::Write(w) = p(&t, "DELETE FROM ships ORDER BY cid, shid LIMIT 2") else {
        panic!()
    };
    assert!(w.limited.is_some() && w.fk.is_none());
    // A parent's are refused (its children would need probing per row).
    match p(&t, "DELETE FROM suppliers ORDER BY sid LIMIT 2") {
        Plan::Reject(r) => assert_eq!(r.reason, Reason::LimitedDml),
        other => panic!("{other:?}"),
    }
    let plain = build_with_fks(
        7,
        &[CatalogGroup {
            id: 3,
            spec: KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap(),
            map: Arc::new(
                ShardMap::presplit(
                    TenantId(7),
                    GroupId(3),
                    KeySpec::new(vec![sprig_shard::KeyClass::Numeric])
                        .unwrap()
                        .digest,
                    4,
                    &[ClusterId::new("c1").unwrap(), ClusterId::new("c2").unwrap()],
                )
                .unwrap(),
            ),
        }],
        &[CatalogTable {
            object: 42,
            db: 1,
            default_schema: DEF.into(),
            name: "ships".into(),
            member: Membership::Sharded {
                group: 3,
                key_columns: Some(vec!["cid".into()]),
                create_sql: Some(SHIPS.into()),
                lookups: Vec::new(),
                fill_rk: false,
                parent_fk: None,
            },
        }],
        &[],
    );
    let Plan::Write(w) = p(
        &plain,
        "DELETE FROM ships WHERE sid > 3 ORDER BY sid DESC, cid, shid LIMIT 2",
    ) else {
        panic!()
    };
    let l = w.limited.unwrap();
    assert_eq!(w.targets.len(), 4);
    assert!(
        w.targets[0].sql.contains(crate::plan::LIMITED_ROWS),
        "{}",
        w.targets[0].sql
    );
    assert!(!w.targets[0].sql.contains("LIMIT"));
    assert!(
        w.targets[0].sql.contains("(sid > 3) AND"),
        "{}",
        w.targets[0].sql
    );
    let Plan::Read(r) = &*l.read else { panic!() };
    assert!(
        r.targets[0]
            .sql
            .contains("WHERE sid > 3 ORDER BY sid DESC, cid, shid LIMIT 2 FOR UPDATE"),
        "{}",
        r.targets[0].sql
    );
    assert_eq!(l.pk, ["cid", "shid"]);
    // Not total: refused.
    match p(&plain, "UPDATE ships SET sid = 1 ORDER BY sid LIMIT 2") {
        Plan::Reject(r) => assert_eq!(r.reason, Reason::LimitedDml),
        other => panic!("{other:?}"),
    }
}
