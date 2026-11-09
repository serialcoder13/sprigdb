#![allow(clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use sprig_common::ids::{ClusterId, GroupId, TenantId};
use sprig_shard::{KeySpec, ShardMap};

use crate::plan::{IdFill, Plan};
use crate::planner::{IdMode, PlanCtx, plan, plan_prepared};
use crate::reject::Reason;
use crate::topology::StaticTopology;
use crate::topology::catalog::{CatalogGroup, CatalogTable, Membership, TableShape, build};

const DEF: &str = "t7_d1_g1_s1";
/// Keyed by its `AUTO_INCREMENT` column.
const TICKETS: &str = "CREATE TABLE `tickets` (\n  `tid` bigint(20) NOT NULL AUTO_INCREMENT,\n  `title` varchar(20) DEFAULT NULL COMMENT 'not AUTO_INCREMENT',\n  PRIMARY KEY (`tid`)\n) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4";
/// Keyed by `cid`; the `AUTO_INCREMENT` column is only the primary key.
const EVENTS: &str = "CREATE TABLE `events` (\n  `eid` int(11) NOT NULL AUTO_INCREMENT,\n  `cid` int(11) NOT NULL,\n  `note` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`cid`,`eid`),\n  KEY `eid` (`eid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
const INVOICES: &str = "CREATE TABLE `invoices` (\n  `iid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  PRIMARY KEY (`iid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
/// A parent member with its own `AUTO_INCREMENT` column.
const NOTES: &str = "CREATE TABLE `notes` (\n  `nid` int(11) NOT NULL AUTO_INCREMENT,\n  `iid` int(11) NOT NULL,\n  `body` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`nid`),\n  KEY `fk_n_inv` (`iid`),\n  CONSTRAINT `fk_n_inv` FOREIGN KEY (`iid`) REFERENCES `invoices` (`iid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

fn topo(shards: u32) -> StaticTopology {
    let c1 = ClusterId::new("c1").unwrap();
    let c2 = ClusterId::new("c2").unwrap();
    let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
    let group = CatalogGroup {
        id: 3,
        spec: spec.clone(),
        map: Arc::new(
            ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, shards, &[c1, c2]).unwrap(),
        ),
    };
    let table =
        |object: u64, name: &str, key: Option<&str>, sql: &str, fk: Option<&str>| CatalogTable {
            object,
            db: 1,
            default_schema: DEF.into(),
            name: name.into(),
            member: Membership::Sharded {
                group: 3,
                key_columns: key.map(|k| vec![k.to_owned()]),
                create_sql: Some(sql.into()),
                lookups: Vec::new(),
                fill_rk: false,
                parent_fk: fk.map(str::to_owned),
            },
        };
    build(
        7,
        &[group],
        &[
            table(41, "tickets", Some("tid"), TICKETS, None),
            table(42, "events", Some("cid"), EVENTS, None),
            table(43, "invoices", Some("cid"), INVOICES, None),
            table(44, "notes", None, NOTES, Some("fk_n_inv")),
        ],
    )
}

fn cx(t: &StaticTopology, zero: bool) -> PlanCtx<'_> {
    PlanCtx {
        topo: t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode {
            no_auto_value_on_zero: zero,
            filled: false,
        },
    }
}

fn need(t: &StaticTopology, sql: &str) -> IdFill {
    match plan(sql, &cx(t, false)) {
        Plan::NeedIds(f) => *f,
        other => panic!("{sql}: expected NeedIds, got {other:?}"),
    }
}

/// Every target's SQL of a finished plan.
fn sqls(p: &Plan) -> Vec<String> {
    match p {
        Plan::Pass(t) => vec![t.sql.clone()],
        Plan::Write(w) => w.targets.iter().map(|t| t.sql.clone()).collect(),
        other => panic!("expected Pass or Write, got {other:?}"),
    }
}

#[test]
fn the_shape_names_the_auto_increment_column() {
    assert_eq!(
        TableShape::parse(TICKETS).auto_increment().as_deref(),
        Some("tid")
    );
    assert_eq!(TableShape::parse(INVOICES).auto_increment(), None);
    let t = topo(4);
    let e = crate::topology::Topology::table(&t, DEF, "events").unwrap();
    let ai = e.auto_inc.as_ref().unwrap();
    assert_eq!(
        (ai.column.as_str(), ai.ordinal, ai.seq),
        ("eid", Some(0), 42)
    );
    assert!(
        crate::topology::Topology::table(&t, DEF, "invoices")
            .unwrap()
            .auto_inc
            .is_none()
    );
}

#[test]
fn rows_that_ask_for_a_value_are_found() {
    let t = topo(4);
    // Left out: the column is added, every row asks.
    let f = need(
        &t,
        "INSERT INTO events (cid, note) VALUES (1, 'a'), (2, 'b')",
    );
    assert_eq!(f.rows, [0, 1]);
    assert_eq!(f.pos, Some(2));
    assert_eq!(f.explicit_max, None);
    assert_eq!(f.seq, 42);
    // NULL, DEFAULT, 0 and '0' ask; explicit values raise.
    let f = need(
        &t,
        "INSERT INTO events (eid, cid) VALUES (NULL, 1), (DEFAULT, 2), (0, 3), ('0', 4), (90, 5), (-3, 6), (7, 7)",
    );
    assert_eq!(f.rows, [0, 1, 2, 3]);
    assert_eq!(f.pos, Some(0));
    assert_eq!(f.explicit_max, Some(90));
    // Without a column list: the column's ordinal.
    let f = need(&t, "INSERT INTO events VALUES (NULL, 1, 'x'), (12, 2, 'y')");
    assert_eq!(
        (f.rows.clone(), f.pos, f.explicit_max),
        (vec![0], Some(0), Some(12))
    );
    // INSERT … SET.
    let f = need(&t, "INSERT INTO events SET cid = 5, note = 'n'");
    assert_eq!((f.rows.clone(), f.pos), (vec![0], None));
    // NO_AUTO_VALUE_ON_ZERO: 0 is a value; nothing to take or raise.
    let p = plan("INSERT INTO events (eid, cid) VALUES (0, 3)", &cx(&t, true));
    assert!(matches!(p, Plan::Pass(_)), "{p:?}");
    // Negative values only: planned at once.
    let p = plan(
        "INSERT INTO events (eid, cid) VALUES (-1, 3)",
        &cx(&t, false),
    );
    assert!(matches!(p, Plan::Pass(_)), "{p:?}");
}

#[test]
fn values_that_are_not_literals_are_refused() {
    let t = topo(4);
    for sql in [
        "INSERT INTO events (eid, cid) VALUES (1 + 1, 3)",
        "INSERT INTO events (eid, cid) VALUES (@x, 3)",
        "INSERT INTO events SET eid = (SELECT 5), cid = 3",
    ] {
        match plan(sql, &cx(&t, false)) {
            Plan::Reject(r) => assert_eq!(r.reason, Reason::AutoIncrement, "{sql}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

#[test]
fn finish_writes_consecutive_values_and_routes_by_them() {
    let t = topo(4);
    let c = cx(&t, false);
    // The column is the key: rows go where their new values hash.
    let f = need(
        &t,
        "INSERT INTO tickets (title) VALUES ('a'), ('b'), ('c'), ('d')",
    );
    let p = f.finish(1001, 1, &c);
    let all = sqls(&p).join(" ");
    for v in 1001..=1004 {
        assert!(all.contains(&format!("', {v})")), "{v}: {all}");
    }
    assert!(!matches!(p, Plan::Reject(_)), "{p:?}");
    // auto_increment_increment = 5: the series steps by 5.
    let f = need(
        &t,
        "INSERT INTO events (cid, note) VALUES (1, 'a'), (1, 'b')",
    );
    let s = sqls(&f.finish(16, 5, &c)).join(" ");
    assert!(
        s.contains("(cid, note, eid) VALUES (1, 'a', 16), (1, 'b', 21)"),
        "{s}"
    );
    // Explicit values stay as written; asking rows get the run in order.
    let f = need(
        &t,
        "INSERT INTO events (eid, cid) VALUES (NULL, 1), (90, 1), (0, 1)",
    );
    let p = f.finish(7, 1, &c);
    let s = sqls(&p).join(" ");
    assert!(s.contains("(7, 1), (90, 1), (8, 1)"), "{s}");
    // INSERT … SET.
    let f = need(&t, "INSERT INTO events SET cid = 5, note = 'n'");
    let s = sqls(&f.finish(33, 1, &c)).join(" ");
    assert!(s.contains("cid = 5, note = 'n', eid = 33"), "{s}");
}

#[test]
fn parent_members_take_ids_then_follow_their_parent() {
    let t = topo(4);
    let f = need(
        &t,
        "INSERT INTO notes (iid, body) VALUES (1, 'a'), (2, 'b')",
    );
    assert_eq!(f.rows, [0, 1]);
    let Plan::ViaParent(v) = f.finish(500, 1, &cx(&t, false)) else {
        panic!()
    };
    let crate::plan::ViaParent { stmt, .. } = *v;
    assert!(
        stmt.to_string()
            .contains("(iid, body, nid) VALUES (1, 'a', 500), (2, 'b', 501)"),
        "{stmt}"
    );
    // One shard: the statement passes with its values.
    let t1 = topo(1);
    let f = need(&t1, "INSERT INTO notes (iid, body) VALUES (1, 'a')");
    let s = sqls(&f.finish(9, 1, &cx(&t1, false))).join(" ");
    assert!(s.contains("(iid, body, nid) VALUES (1, 'a', 9)"), "{s}");
}

#[test]
fn upserts_replace_ignore_and_prepared_inserts_take_ids() {
    let t = topo(4);
    for sql in [
        "REPLACE INTO tickets (title) VALUES ('a')",
        "INSERT IGNORE INTO tickets (title) VALUES ('a'), ('b')",
        "INSERT INTO tickets (tid, title) VALUES (NULL, 'a') ON DUPLICATE KEY UPDATE title = 'b'",
    ] {
        let f = need(&t, sql);
        let p = f.finish(70, 1, &cx(&t, false));
        assert!(!matches!(p, Plan::Reject(_)), "{sql}: {p:?}");
        assert!(sqls(&p).join(" ").contains("70"), "{sql}: {p:?}");
    }
    let tpl = crate::planner::parse_template("INSERT INTO events (cid, note) VALUES (?, ?)", &t)
        .unwrap()
        .unwrap();
    let pp = plan_prepared(
        &tpl,
        &[
            crate::planner::ParamLit::Int(3),
            crate::planner::ParamLit::Null,
        ],
        &cx(&t, false),
    );
    assert!(!pp.placeholders);
    let Plan::NeedIds(f) = pp.plan else {
        panic!("{:?}", pp.plan)
    };
    let s = sqls(&f.finish(12, 1, &cx(&t, false))).join(" ");
    assert!(s.contains("(cid, note, eid) VALUES (3, NULL, 12)"), "{s}");
}

#[test]
fn explain_shows_the_id_blocks() {
    let t = topo(4);
    let p = plan(
        "EXPLAIN FORMAT=SPRIG INSERT INTO tickets (title) VALUES ('a')",
        &cx(&t, false),
    );
    let Plan::Explain(inner) = p else {
        panic!("{p:?}")
    };
    assert!(matches!(*inner, Plan::NeedIds(_)));
    let p = plan(
        "EXPLAIN INSERT INTO tickets (title) VALUES ('a')",
        &cx(&t, false),
    );
    let Plan::Pass(x) = p else { panic!("{p:?}") };
    assert!(x.sql.starts_with("EXPLAIN INSERT INTO"), "{}", x.sql);
}
