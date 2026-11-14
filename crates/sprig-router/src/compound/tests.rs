#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::planner::{IdMode, plan};
use crate::topology::{StaticTopology, Topology, TopologyFile};

const DEF: &str = "t1_d2_g1_s1";

/// A Redis database `db0` grouped on `_sprig_dk` (group 7, two shards on
/// two clusters) with its element tables, and an ordinary table.
fn topo() -> StaticTopology {
    let f: TopologyFile = serde_json::from_str(
        r#"{"tenants": {"a": {
          "groups": [
            {"id": 7, "key_class": "binary", "shards": [
              {"shard": 1, "schema": "t1_d2_g7_s1", "cluster": "c1"},
              {"shard": 2, "schema": "t1_d2_g7_s2", "cluster": "c2"}]},
            {"id": 9, "key_class": "int", "shards": [
              {"shard": 1, "schema": "t1_d2_g9_s1", "cluster": "c1"}]}],
          "tables": [
            {"schema": "t1_d2_g1_s1", "table": "db0", "group": 7, "key": "_sprig_dk", "upsert_safe": true},
            {"schema": "t1_d2_g1_s1", "table": "__sprig_ke_db0", "group": 7, "key": "_sprig_dk", "upsert_safe": true},
            {"schema": "t1_d2_g1_s1", "table": "__sprig_kz_db0", "group": 7, "key": "_sprig_dk", "upsert_safe": true},
            {"schema": "t1_d2_g1_s1", "table": "orders", "group": 9, "key": "id"}]}}}"#,
    )
    .unwrap();
    f.tenants["a"].build().unwrap()
}

fn cx(t: &StaticTopology) -> PlanCtx<'_> {
    PlanCtx {
        topo: t,
        default_schema: Some(DEF),
        session_cluster: "c1",
        max_route_values: 1024,
        lookup_batch: 256,
        ids: IdMode::default(),
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::from("X'");
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s.push('\'');
    s
}

/// The shard schema owning distribution key `dk`.
fn owner(t: &StaticTopology, dk: &[u8]) -> (String, String) {
    let g = &t.table(DEF, "db0").unwrap().group;
    let s = &g.shards[g.shard_of(&KeyValue::Bytes(dk.to_vec())).unwrap()];
    (s.schema.clone(), s.cluster.clone())
}

/// Two keys whose distribution keys land on different shards.
fn two_keys(t: &StaticTopology) -> (Vec<u8>, Vec<u8>) {
    let first = owner(t, b"k0").0;
    for i in 1..1000 {
        let k = format!("k{i}").into_bytes();
        if owner(t, &k).0 != first {
            return (b"k0".to_vec(), k);
        }
    }
    panic!("every key on one shard");
}

fn batch(dk: &[u8]) -> String {
    let k = hex(dk);
    format!(
        "BEGIN NOT ATOMIC START TRANSACTION; \
         SELECT 1, t.type INTO @now, @t FROM (SELECT 1 AS one) AS d LEFT JOIN \
         `t1_d2_g1_s1`.`db0` AS t ON t.k = {k} AND t._sprig_dk = {k} FOR UPDATE; \
         IF @x THEN DELETE FROM `t1_d2_g1_s1`.`db0` WHERE k = {k}; \
         DELETE FROM `t1_d2_g1_s1`.`__sprig_ke_db0` WHERE k = {k} AND g = @g; END IF; \
         INSERT INTO `t1_d2_g1_s1`.`db0` (k, type, _sprig_dk, _sprig_rk) VALUES ({k}, 'string', {k}, 7); \
         COMMIT; SELECT @rc, 'a.b = 1 `t1_d2_g1_s1`.`db0`'; END"
    )
}

#[test]
fn a_keyed_batch_goes_to_its_keys_shard() {
    let t = topo();
    let (a, b) = two_keys(&t);
    for dk in [a, b] {
        let (schema, cluster) = owner(&t, &dk);
        let Some(Plan::Pass(p)) = plan_keyed_compound(&batch(&dk), &cx(&t)) else {
            panic!("not a pass");
        };
        assert_eq!(p.cluster, cluster);
        assert_eq!(p.at.schema, schema);
        assert_eq!(p.sql.matches(&format!("`{schema}`.`db0`")).count(), 3);
        assert_eq!(
            p.sql
                .matches(&format!("`{schema}`.`__sprig_ke_db0`"))
                .count(),
            1
        );
        // Text inside a string literal is left alone.
        assert!(p.sql.contains("'a.b = 1 `t1_d2_g1_s1`.`db0`'"), "{}", p.sql);
        assert!(!p.sql.contains("`t1_d2_g1_s1`.`db0` AS"), "{}", p.sql);
    }
}

#[test]
fn other_compounds_are_not_routed_here() {
    let t = topo();
    let (a, b) = two_keys(&t);
    // Two distribution keys.
    let two = batch(&a).replace(
        "COMMIT;",
        &format!(
            "DELETE FROM `t1_d2_g1_s1`.`db0` WHERE k = {0} AND _sprig_dk = {0}; COMMIT;",
            hex(&b)
        ),
    );
    assert_eq!(plan_keyed_compound(&two, &cx(&t)), None);
    // The same key twice is fine.
    let same = batch(&a).replace(
        "COMMIT;",
        &format!(
            "DELETE FROM `t1_d2_g1_s1`.`db0` WHERE k = {0} AND _sprig_dk = {0}; COMMIT;",
            hex(&a)
        ),
    );
    assert!(plan_keyed_compound(&same, &cx(&t)).is_some());
    // No distribution key.
    let none = "BEGIN NOT ATOMIC DELETE FROM `t1_d2_g1_s1`.`db0` WHERE k = X'01'; END";
    assert_eq!(plan_keyed_compound(none, &cx(&t)), None);
    // A table keyed by another column.
    let other = format!(
        "BEGIN NOT ATOMIC DELETE FROM `t1_d2_g1_s1`.`orders` WHERE id = 1 AND _sprig_dk = {}; END",
        hex(&a)
    );
    assert_eq!(plan_keyed_compound(&other, &cx(&t)), None);
    // Not a compound statement.
    let plain = format!(
        "DELETE FROM `t1_d2_g1_s1`.`db0` WHERE k = {0} AND _sprig_dk = {0}",
        hex(&a)
    );
    assert_eq!(plan_keyed_compound(&plain, &cx(&t)), None);
    assert!(is_compound("  /* x */ begin not atomic select 1; end"));
    assert!(!is_compound("BEGIN; SELECT 1"));
}

fn pass(t: &StaticTopology, sql: &str) -> Target {
    match plan(sql, &cx(t)) {
        Plan::Pass(p) => p,
        other => panic!("{sql}: expected Pass, got {other:?}"),
    }
}

/// The statement path's shapes (`sprig_redis::ctx` and `cmd`) plan as one
/// shard's statements.
#[test]
fn the_statement_paths_shapes_go_to_one_shard() {
    let t = topo();
    let (a, b) = two_keys(&t);
    for dk in [&a, &b] {
        let (schema, cluster) = owner(&t, dk);
        let k = hex(dk);
        let kp = format!("k = {k} AND _sprig_dk = {k}");
        for sql in [
            format!(
                "SELECT 1, MAX(t.type), MAX(t.v) FROM `t1_d2_g1_s1`.`db0` AS t WHERE t.k = {k} AND t._sprig_dk = {k} FOR UPDATE"
            ),
            format!(
                "SELECT t.type, e.e, e.v FROM `t1_d2_g1_s1`.`db0` AS t LEFT JOIN `t1_d2_g1_s1`.`__sprig_ke_db0` AS e \
                 ON e.k = t.k AND e.g = t.g AND e._sprig_dk = t._sprig_dk WHERE t.k = {k} AND t._sprig_dk = {k} ORDER BY e.o, e.e"
            ),
            format!("DELETE FROM `t1_d2_g1_s1`.`db0` WHERE {kp}"),
            format!("UPDATE `t1_d2_g1_s1`.`db0` SET n = 1, _v = 5 WHERE {kp}"),
            format!(
                "UPDATE `t1_d2_g1_s1`.`__sprig_ke_db0` SET o = o - 1 WHERE {kp} AND g = 3 AND o < 5 ORDER BY o ASC"
            ),
            format!(
                "INSERT INTO `t1_d2_g1_s1`.`__sprig_ke_db0` (k, g, o, e, v, _sprig_dk, _sprig_rk) \
                 VALUES ({k}, 3, 1, X'61', X'62', {k}, 9), ({k}, 3, 2, X'63', X'64', {k}, 9) \
                 ON DUPLICATE KEY UPDATE v = VALUES(v)"
            ),
            format!(
                "INSERT INTO `t1_d2_g1_s1`.`db0` (k, type, v, n, lh, lt, g, _v, expires_at, _sprig_dk, _sprig_rk) \
                 VALUES ({k}, 'string', X'01', 0, 0, 0, 1, 1, NULL, {k}, 9)"
            ),
            format!("SELECT _v FROM `t1_d2_g1_s1`.`db0` WHERE {kp} FOR UPDATE"),
            format!("SELECT 1, COUNT(*) FROM `t1_d2_g1_s1`.`db0` WHERE {kp}"),
        ] {
            let p = pass(&t, &sql);
            assert_eq!(p.cluster, cluster, "{sql}");
            assert!(p.sql.contains(&schema), "{sql}: {}", p.sql);
            // Every table goes to the key's shard.
            assert!(!p.sql.contains(DEF), "{sql}: {}", p.sql);
        }
        // The earlier key read (a derived table joined to the key row) is
        // refused on a sharded database, hence the single-table forms.
        let old = format!(
            "SELECT 1, t.type FROM (SELECT 1 AS one) AS d LEFT JOIN `t1_d2_g1_s1`.`db0` AS t \
             ON t.k = {k} AND t._sprig_dk = {k} FOR UPDATE"
        );
        assert!(matches!(plan(&old, &cx(&t)), Plan::Reject(_)));
    }
}
