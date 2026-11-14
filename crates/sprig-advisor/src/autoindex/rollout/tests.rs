//! Unit tests of rollout: method choice, windows, slots, retries and the
//! `PROCESSLIST` rules.

#![allow(clippy::unwrap_used)]

use std::time::{Duration, UNIX_EPOCH};

use sprig_cluster::driver::DdlMethod;
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::{ClusterId, NodeId, ObjectId};

use super::driver::next_window;
use super::node::{
    Stop, Thread, auto_ddl, drop_sql, ignore_sql, is_sprig_build, is_user_ddl, judge_threads,
};
use super::*;
use crate::autoindex::model::IndexDef;
use crate::autoindex::store::IndexState;

fn ep(n: &str) -> NodeEndpoint {
    NodeEndpoint::from_url(NodeId::new(n).unwrap(), "mysql://u:p@127.0.0.1:1").unwrap()
}

fn view(nodes: Nodes, ddl: DdlMethod) -> ClusterView {
    ClusterView {
        cluster: ClusterId::new("mum1-c1").unwrap(),
        nodes,
        ddl_method: ddl,
        instant_ignored_toggle: true,
        unhealthy: None,
    }
}

fn galera(n: usize) -> Nodes {
    Nodes::Galera {
        nodes: (1..=n).map(|i| ep(&format!("g{i}"))).collect(),
        writer: NodeId::new("g1").unwrap(),
    }
}

fn t() -> ShardTable {
    ShardTable {
        shard: "0".into(),
        schema: "t42_d1_g0_s0".into(),
        table: "orders".into(),
    }
}

#[test]
fn method_per_topology() {
    let p = GaleraPolicy::default();
    let single = view(Nodes::Single { writer: ep("s1") }, DdlMethod::Online);
    assert_eq!(
        choose_method(&single, 1 << 40, &p, false),
        BuildMethod::Online
    );
    let no_online = view(Nodes::Single { writer: ep("s1") }, DdlMethod::CopySwap);
    assert!(matches!(
        choose_method(&no_online, 1, &p, false),
        BuildMethod::Recommend(_)
    ));
    assert_eq!(
        choose_method(&no_online, 1, &p, true),
        BuildMethod::CopySwap
    );

    // The default: TOI for small tables, a recommendation above.
    assert_eq!(p.switch, GaleraSwitch::ToiOnly);
    let g = view(galera(3), DdlMethod::CopySwap);
    assert_eq!(
        choose_method(&g, 10 << 20, &p, false),
        BuildMethod::GaleraToi
    );
    assert!(matches!(
        choose_method(&g, 65 << 20, &p, false),
        BuildMethod::Recommend(_)
    ));
    let auto = GaleraPolicy {
        switch: GaleraSwitch::Auto,
        ..p
    };
    assert_eq!(
        choose_method(&g, 10 << 20, &auto, false),
        BuildMethod::GaleraToi
    );
    assert_eq!(
        choose_method(&g, 65 << 20, &auto, false),
        BuildMethod::GaleraRsu
    );
    // Too few nodes for rolling RSU.
    let g2 = view(galera(2), DdlMethod::CopySwap);
    assert!(matches!(
        choose_method(&g2, 65 << 20, &auto, false),
        BuildMethod::Recommend(_)
    ));
    assert_eq!(
        choose_method(&g2, 1 << 20, &auto, false),
        BuildMethod::GaleraToi
    );
    // The switch the mover replaces.
    let mover = GaleraPolicy {
        switch: GaleraSwitch::Mover,
        ..p
    };
    assert_eq!(choose_method(&g, 1, &mover, true), BuildMethod::CopySwap);
    assert!(matches!(
        choose_method(&g, 1, &mover, false),
        BuildMethod::Recommend(_)
    ));
    let toi = GaleraPolicy {
        switch: GaleraSwitch::ToiOnly,
        ..p
    };
    assert!(matches!(
        choose_method(&g, 65 << 20, &toi, false),
        BuildMethod::Recommend(_)
    ));
    let rsu = GaleraPolicy {
        switch: GaleraSwitch::RollingRsu,
        ..p
    };
    assert_eq!(choose_method(&g, 1, &rsu, false), BuildMethod::GaleraRsu);
    let off = GaleraPolicy {
        switch: GaleraSwitch::RecommendOnly,
        ..p
    };
    assert!(matches!(
        choose_method(&g, 1, &off, false),
        BuildMethod::Recommend(_)
    ));
}

fn at(h: u64, m: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(20_000 * 86_400 + h * 3600 + m * 60)
}

#[test]
fn windows() {
    let w = BuildWindow::parse("01:00-05:00").unwrap();
    assert!(window_gate(w, None, at(2, 0)).is_ok());
    assert_eq!(
        window_gate(w, None, at(0, 30)),
        Err(Duration::from_secs(30 * 60))
    );
    assert_eq!(
        window_gate(w, None, at(5, 0)),
        Err(Duration::from_secs(20 * 3600))
    );
    // Wrapping midnight.
    let night = BuildWindow::parse("22:00-02:00").unwrap();
    assert!(window_gate(night, None, at(23, 0)).is_ok());
    assert!(window_gate(night, None, at(1, 59)).is_ok());
    assert!(window_gate(night, None, at(2, 0)).is_err());
    // auto: open until the scheduler knows the quiet hours, then those.
    assert!(window_gate(BuildWindow::Auto, None, at(12, 0)).is_ok());
    assert!(window_gate(BuildWindow::Auto, Some(w), at(12, 0)).is_err());
    // The next window after lock timeouts.
    assert_eq!(
        next_window(w, None, at(2, 0)),
        Duration::from_secs(23 * 3600)
    );
    assert_eq!(next_window(w, None, at(0, 59)), Duration::from_secs(60));
    assert_eq!(
        next_window(BuildWindow::Auto, None, at(2, 0)),
        Duration::from_secs(3600)
    );
}

#[test]
fn least_busy_four_hours() {
    let mut load = [10.0; 24];
    for h in [3, 4, 5, 6] {
        load[h] = 1.0;
    }
    assert_eq!(
        least_busy_window(&load),
        BuildWindow::Fixed {
            start: 180,
            end: 420
        }
    );
    // Wrapping midnight.
    let mut load = [10.0; 24];
    for h in [22, 23, 0, 1] {
        load[h] = 0.0;
    }
    assert_eq!(
        least_busy_window(&load),
        BuildWindow::Fixed {
            start: 22 * 60,
            end: 120
        }
    );
}

#[test]
fn slots_count_holders_and_builds_elsewhere() {
    let s = LocalSlots::new();
    let c = ClusterId::new("mum1-c1").unwrap();
    let a = s.try_acquire(&c, 1, 0).unwrap();
    assert!(s.try_acquire(&c, 1, 0).is_none());
    assert_eq!(s.held(&c), 1);
    drop(a);
    assert_eq!(s.held(&c), 0);
    // A build seen running on the cluster takes the only slot.
    assert!(s.try_acquire(&c, 1, 1).is_none());
    let two = s.try_acquire(&c, 2, 1).unwrap();
    assert!(s.try_acquire(&c, 2, 1).is_none());
    drop(two);
    // Other clusters are independent.
    let d = ClusterId::new("mum1-c2").unwrap();
    let _x = s.try_acquire(&c, 1, 0).unwrap();
    assert!(s.try_acquire(&d, 1, 0).is_some());
}

#[test]
fn progress_round_trips_in_observed() {
    let mut p = Progress::default();
    assert_eq!(Progress::of(None), p);
    p.failures = 2;
    p.not_before_ms = 77;
    let other = serde_json::json!({"usage": {"method": "explain"}});
    let o = p.into_observed(Some(&other));
    assert_eq!(o["usage"]["method"], "explain");
    assert_eq!(Progress::of(Some(&o)), p);
}

#[test]
fn user_ddl_is_recognized() {
    let (s, tb) = ("t42_d1_g0_s0", "orders");
    assert!(is_user_ddl(
        "ALTER TABLE `t42_d1_g0_s0`.`orders` ADD COLUMN x INT",
        None,
        s,
        tb
    ));
    assert!(is_user_ddl(
        "alter table orders add column x int",
        Some(s),
        s,
        tb
    ));
    assert!(is_user_ddl("CREATE INDEX i ON orders (a)", Some(s), s, tb));
    assert!(is_user_ddl("DROP TABLE t42_d1_g0_s0.orders", None, s, tb));
    assert!(is_user_ddl("RENAME TABLE orders TO o2", Some(s), s, tb));
    assert!(is_user_ddl("TRUNCATE orders", Some(s), s, tb));
    // Another schema, another table, DML, our own builds.
    assert!(!is_user_ddl(
        "alter table orders add column x int",
        Some("other"),
        s,
        tb
    ));
    assert!(!is_user_ddl(
        "ALTER TABLE t42_d1_g0_s0.orders2 ADD x INT",
        None,
        s,
        tb
    ));
    assert!(!is_user_ddl("UPDATE orders SET a = 1", Some(s), s, tb));
    assert!(!is_user_ddl(
        "ALTER TABLE `t42_d1_g0_s0`.`orders` ADD INDEX IF NOT EXISTS `sprig_auto_0a1b2c3d` (`a`)",
        None,
        s,
        tb
    ));
    assert!(is_sprig_build(
        "ALTER TABLE `s`.`t` ADD INDEX IF NOT EXISTS `sprig_auto_0a1b2c3d` (`a`)"
    ));
    assert!(!is_sprig_build(
        "ALTER TABLE `s`.`t` ADD INDEX `idx_a` (`a`)"
    ));
}

fn th(id: u64, state: &str, ms: f64, info: &str) -> Thread {
    Thread {
        id,
        db: Some("t42_d1_g0_s0".into()),
        command: "Query".into(),
        state: state.into(),
        time_ms: ms,
        info: Some(info.into()),
    }
}

#[test]
fn processlist_rules() {
    let t = t();
    let after = Duration::from_secs(1);
    let build =
        "ALTER TABLE `t42_d1_g0_s0`.`orders` ADD INDEX IF NOT EXISTS `sprig_auto_0a1b2c3d` (`a`)";
    let mdl = "Waiting for table metadata lock";
    let insert = "INSERT INTO orders VALUES (1)";
    // Waiting for the first lock with clients queued behind: killed after 1 s.
    let mut started = false;
    let v = [th(7, mdl, 400.0, build), th(8, mdl, 300.0, insert)];
    assert_eq!(judge_threads(&v, 7, &t, &mut started, after), None);
    let v = [th(7, mdl, 1500.0, build), th(8, mdl, 1400.0, insert)];
    assert_eq!(
        judge_threads(&v, 7, &t, &mut started, after).unwrap().0,
        Stop::LockQueue
    );
    // Nobody queued: lock_wait_timeout decides.
    let v = [th(7, mdl, 4000.0, build)];
    assert_eq!(judge_threads(&v, 7, &t, &mut started, after), None);
    // Once it ran, the final lock upgrade is left to lock_wait_timeout.
    let v = [th(7, "altering table", 2000.0, build)];
    assert_eq!(judge_threads(&v, 7, &t, &mut started, after), None);
    assert!(started);
    let v = [th(7, mdl, 9000.0, build), th(8, mdl, 3000.0, insert)];
    assert_eq!(judge_threads(&v, 7, &t, &mut started, after), None);
    // User DDL wins, running or queued.
    let v = [
        th(7, "altering table", 100.0, build),
        th(9, mdl, 10.0, "ALTER TABLE orders ADD c INT"),
    ];
    assert_eq!(
        judge_threads(&v, 7, &t, &mut started, after).unwrap().0,
        Stop::UserDdl
    );
}

#[test]
fn only_automatic_indexes_are_altered() {
    let t = t();
    assert!(auto_ddl(&t, "idx_user", "DROP INDEX idx_user").is_err());
    assert!(drop_sql(&t, "PRIMARY").is_err());
    assert!(ignore_sql(&t, "sprig_auto_12345678x", true).is_err());
    assert_eq!(
        drop_sql(&t, "sprig_auto_0a1b2c3d").unwrap(),
        "ALTER TABLE `t42_d1_g0_s0`.`orders` DROP INDEX IF EXISTS `sprig_auto_0a1b2c3d`"
    );
    assert_eq!(
        ignore_sql(&t, "sprig_auto_0a1b2c3d", false).unwrap(),
        "ALTER TABLE `t42_d1_g0_s0`.`orders` ALTER INDEX `sprig_auto_0a1b2c3d` NOT IGNORED"
    );
    let def = IndexDef::btree(ObjectId(9), &["a", "b"]);
    assert_eq!(
        add_index_clause(&def),
        format!("ADD INDEX IF NOT EXISTS `{}` (`a`, `b`)", def.name())
    );
}

#[test]
fn the_state_machine_has_the_rollout_and_observer_edges() {
    use IndexState as S;
    // Rollout.
    assert!(S::RollingOut.can_move_to(S::Observing));
    assert!(S::RollingOut.can_move_to(S::Failed));
    assert!(!S::RollingOut.can_move_to(S::Active));
    assert!(!S::RollingOut.can_move_to(S::Recommended));
    // Observation and retirement.
    assert!(S::Observing.can_move_to(S::Active));
    assert!(S::Observing.can_move_to(S::Retiring));
    assert!(S::Active.can_move_to(S::Retiring));
    assert!(S::Retiring.can_move_to(S::Active));
    assert!(S::Retiring.can_move_to(S::Dropping));
    assert!(S::Dropping.can_move_to(S::Dropped));
    // `failed` is terminal except for clean-up: `failed → dropping` is an
    // edge (the observer may clean a failed build's shards that way);
    // nothing else leaves `failed`.
    assert!(S::Failed.can_move_to(S::Dropping));
    assert!(!S::Failed.can_move_to(S::RollingOut));
    // Drift and dropped tables reach dropped from any live state.
    assert!(S::Active.can_move_to(S::Dropped));
    assert!(!S::Frozen.can_move_to(S::Dropped));
}
