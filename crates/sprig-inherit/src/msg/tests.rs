use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use sprig_common::ids::TenantId;
use sprig_sql::lex::LexOptions;

use super::grammar::{self, AlterOp, For, ShowInherited, Stmt, Target};
use super::model::*;
use super::store::{datetime, tenant_of_schema};

fn t(n: u64) -> TenantId {
    TenantId(n)
}

#[test]
fn scope_membership() {
    // 1 → 2 → 3 → 4, and 1 → 5.
    let anc4 = [t(1), t(2), t(3)];
    let anc5 = [t(1)];
    let all = Scope::default();
    assert!(all.reaches(t(1), t(4), &anc4));
    assert!(all.reaches(t(1), t(5), &anc5));
    assert!(
        !all.reaches(t(1), t(1), &[]),
        "not itself without INCLUDING SELF"
    );
    assert!(!all.reaches(t(2), t(5), &anc5), "a sibling subtree");
    let me = Scope {
        including_self: true,
        ..Scope::default()
    };
    assert!(me.reaches(t(1), t(1), &[]));
    let kids = Scope {
        depth: Depth::Children,
        ..Scope::default()
    };
    assert!(kids.reaches(t(1), t(5), &anc5));
    assert!(!kids.reaches(t(1), t(4), &anc4));
    assert!(kids.reaches(t(3), t(4), &anc4));
    let under = Scope {
        under: Some(3),
        ..Scope::default()
    };
    assert!(under.reaches(t(1), t(4), &anc4));
    assert!(under.reaches(t(1), t(3), &[t(1), t(2)]));
    assert!(!under.reaches(t(1), t(2), &[t(1)]));
    assert!(!under.reaches(t(1), t(5), &anc5));
}

fn def(declarer: u64, object: u64, kind: DefKind, name: &str, scope: Scope) -> Arc<Definition> {
    Arc::new(Definition {
        declarer: t(declarer),
        object_id: object,
        kind,
        db_name: "security".into(),
        name: name.into(),
        scope,
        definition: json!({}),
        grants: None,
        def_version: 1,
        state: DefState::Active,
        drop_after_ms: None,
        created_by: 1,
        created_at_ms: 0,
        changed_version: 1,
    })
}

fn state_with(defs: &[Arc<Definition>], overrides: Vec<Override>) -> Arc<MsgState> {
    let mut s = MsgState {
        migrated: true,
        ..MsgState::default()
    };
    for d in defs {
        s.defs
            .insert((db_key(&d.db_name), d.kind, d.name.clone()), Arc::clone(d));
    }
    s.overrides = overrides.into_iter().map(Arc::new).collect();
    Arc::new(s)
}

#[test]
fn nearest_definition_wins_and_shadows() {
    // system(1) → app(2) → client(3): both 1 and 2 declare user_events.
    let d1 = def(1, 10, DefKind::Stream, "user_events", Scope::default());
    let d2 = def(2, 20, DefKind::Stream, "user_events", Scope::default());
    let chain = Chain {
        levels: vec![
            Level {
                tenant: t(3),
                state: None,
            },
            Level {
                tenant: t(2),
                state: Some(state_with(&[d2], Vec::new())),
            },
            Level {
                tenant: t(1),
                state: Some(state_with(&[d1], Vec::new())),
            },
        ],
    };
    let r = chain
        .resolve("Security", DefKind::Stream, "user_events")
        .unwrap();
    assert_eq!(
        r.key(),
        DefKey {
            tenant: t(2),
            object: 20
        }
    );
    assert_eq!(chain.reaching(DefKind::Stream).len(), 1);
    assert!(
        chain
            .resolve("security", DefKind::Queue, "user_events")
            .is_none()
    );
    assert!(chain.is_proper_ancestor(t(1)));
    assert!(!chain.is_proper_ancestor(t(3)));
}

/// A tiny deterministic generator (no `rand` in this crate).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// The reference model of chapter 42's override rule, written as a rank
/// over every visible row: `(class, distance of the target, distance of
/// the maker)`, smallest wins; class 0 targets the member itself, class 1
/// is a subtree override of an ancestor between the member and the
/// declarer.
fn model(
    parent: &BTreeMap<u64, u64>,
    rows: &[Override],
    member: u64,
    declarer: u64,
    key: &str,
) -> Option<serde_json::Value> {
    let dist = |from: u64, to: u64| -> Option<usize> {
        // Steps from `from` up to its ancestor-or-self `to`.
        let mut x = from;
        let mut d = 0;
        loop {
            if x == to {
                return Some(d);
            }
            x = *parent.get(&x)?;
            d += 1;
        }
    };
    let limit = dist(member, declarer)?;
    rows.iter()
        .filter(|o| o.value.get(key).is_some())
        .filter_map(|o| {
            let made = dist(member, o.made_in.0)?;
            let target = dist(member, o.target.0)?;
            let class = if target == 0 {
                0
            } else if o.applies_to == AppliesTo::Subtree && target <= limit {
                1
            } else {
                return None;
            };
            Some(((class, target, made), o.value[key].clone()))
        })
        .min_by_key(|(r, _)| *r)
        .map(|(_, v)| v)
}

#[test]
fn override_resolution_matches_the_model() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut checked = 0;
    for _round in 0..300 {
        // A random tree of up to 40 tenants, depth up to 6.
        let n = 2 + rng.below(39);
        let mut parent: BTreeMap<u64, u64> = BTreeMap::new();
        let mut depth: BTreeMap<u64, u64> = BTreeMap::new();
        depth.insert(1, 0);
        for id in 2..=n {
            let candidates: Vec<u64> = (1..id).filter(|p| depth[p] < 5).collect();
            let p = candidates[usize::try_from(rng.below(candidates.len() as u64)).unwrap()];
            parent.insert(id, p);
            depth.insert(id, depth[&p] + 1);
        }
        let chain_of = |mut x: u64| {
            let mut v = vec![x];
            while let Some(p) = parent.get(&x) {
                v.push(*p);
                x = *p;
            }
            v
        };
        let declarer = 1 + rng.below(n);
        let key = DefKey {
            tenant: t(declarer),
            object: 7,
        };
        // Random overrides: made in X, for a tenant in X's subtree.
        let mut rows = Vec::new();
        for _ in 0..rng.below(25) {
            let target = 1 + rng.below(n);
            let ups = chain_of(target);
            let made_in = ups[usize::try_from(rng.below(ups.len() as u64)).unwrap()];
            let k = if rng.below(2) == 0 { "a" } else { "b" };
            rows.push(Override {
                made_in: t(made_in),
                def: key,
                target: t(target),
                applies_to: if rng.below(2) == 0 {
                    AppliesTo::Own
                } else {
                    AppliesTo::Subtree
                },
                what: What::Settings,
                value: json!({ k: rng.below(1000) }),
                set_by: 1,
                set_at_ms: 0,
            });
        }
        // Every member: compare the chain's answer with the model's.
        for member in 1..=n {
            let ups = chain_of(member);
            if member == declarer || !ups.contains(&declarer) {
                continue;
            }
            let levels = ups
                .iter()
                .map(|x| {
                    let mine: Vec<Override> =
                        rows.iter().filter(|o| o.made_in.0 == *x).cloned().collect();
                    Level {
                        tenant: t(*x),
                        state: Some(state_with(&[], mine)),
                    }
                })
                .collect();
            let chain = Chain { levels };
            for k in ["a", "b"] {
                let got = chain
                    .effective(key, What::Settings, Some(k))
                    .map(|(v, _)| v);
                assert_eq!(
                    got,
                    model(&parent, &rows, member, declarer, k),
                    "member {member}, declarer {declarer}, key {k}, rows {rows:#?}"
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 1000, "{checked}");
}

#[test]
fn disabled_follows_the_nearest_state() {
    let key = DefKey {
        tenant: t(1),
        object: 9,
    };
    let o = |made: u64, target: u64, a: AppliesTo, v: &str| Override {
        made_in: t(made),
        def: key,
        target: t(target),
        applies_to: a,
        what: What::State,
        value: json!(v),
        set_by: 1,
        set_at_ms: 0,
    };
    // 1 → 2 → 3: 2 disables its subtree; 3 enables itself.
    let chain = |rows3: Vec<Override>, rows2: Vec<Override>| Chain {
        levels: vec![
            Level {
                tenant: t(3),
                state: Some(state_with(&[], rows3)),
            },
            Level {
                tenant: t(2),
                state: Some(state_with(&[], rows2)),
            },
            Level {
                tenant: t(1),
                state: Some(state_with(&[], Vec::new())),
            },
        ],
    };
    let c = chain(Vec::new(), vec![o(2, 2, AppliesTo::Subtree, "disabled")]);
    assert!(c.disabled(key));
    let c = chain(
        vec![o(3, 3, AppliesTo::Own, "enabled")],
        vec![o(2, 2, AppliesTo::Subtree, "disabled")],
    );
    assert!(!c.disabled(key));
    // An override 2 made for 3 alone counts as 3's own.
    let c = chain(Vec::new(), vec![o(2, 3, AppliesTo::Own, "disabled")]);
    assert!(c.disabled(key));
    // 3's own beats 2's for 3.
    let c = chain(
        vec![o(3, 3, AppliesTo::Own, "enabled")],
        vec![o(2, 3, AppliesTo::Own, "disabled")],
    );
    assert!(!c.disabled(key));
}

fn opts() -> LexOptions {
    LexOptions::mariadb()
}

#[test]
fn for_each_clause() {
    let sql =
        "CREATE STREAM user_events FOR EACH TENANT KEY (user_id) PARTITIONS 6 RETENTION '7 days'";
    let f = grammar::for_each(sql, &opts()).unwrap().unwrap();
    assert_eq!(f.depth, Depth::Descendants);
    assert_eq!(
        f.strip(sql)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        "CREATE STREAM user_events KEY (user_id) PARTITIONS 6 RETENTION '7 days'"
    );
    let sql = "CREATE TRIGGER sync_users ON STREAM security.user_events FOR EACH TENANT \
               CHILDREN UNDER client_x INCLUDING SELF GRANTS (INSERT, UPDATE ON app_a.users, \
               SELECT ON app_b.*) LANGUAGE javascript AS $$ for each tenant $$";
    let f = grammar::for_each(sql, &opts()).unwrap().unwrap();
    assert_eq!(f.depth, Depth::Children);
    assert_eq!(f.under.as_deref(), Some("client_x"));
    assert!(f.including_self);
    assert_eq!(
        f.grants,
        vec![
            GrantItem {
                privileges: vec!["INSERT".into(), "UPDATE".into()],
                db: "app_a".into(),
                name: "users".into()
            },
            GrantItem {
                privileges: vec!["SELECT".into()],
                db: "app_b".into(),
                name: "*".into()
            },
        ]
    );
    assert!(
        f.strip(sql)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .contains("security.user_events LANGUAGE javascript")
    );
    // Only before the body.
    assert!(
        grammar::for_each(
            "CREATE TRIGGER g ON STREAM s LANGUAGE lua AS 'FOR EACH TENANT'",
            &opts()
        )
        .unwrap()
        .is_none()
    );
    assert!(grammar::for_each("CREATE STREAM s FOR EACH TENANT WITH APP x", &opts()).is_err());
    assert!(
        grammar::for_each(
            "CREATE TRIGGER g ON STREAM s FOR EACH TENANT GRANTS (INSERT ON t)",
            &opts()
        )
        .is_err()
    );
}

#[test]
fn insert_targets() {
    let sql = "INSERT INTO user_events FOR TENANT client_x (key, value) VALUES (1, 'a')";
    let it = grammar::insert_target(sql, &opts()).unwrap().unwrap();
    assert_eq!(it.target, Target::Tenant("client_x".into()));
    assert_eq!(it.name, "user_events");
    assert_eq!(
        it.strip(sql)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        "INSERT INTO user_events (key, value) VALUES (1, 'a')"
    );
    let sql = "insert ignore into security.user_events for all tenants with instance under 'b' values (1, 2)";
    let it = grammar::insert_target(sql, &opts()).unwrap().unwrap();
    assert_eq!(it.db.as_deref(), Some("security"));
    assert_eq!(
        it.target,
        Target::All {
            with_instance: true,
            under: Some("b".into())
        }
    );
    let sql = "INSERT INTO platform.client_events IN TENANT system (key, value) VALUES ('x', '{}')";
    let it = grammar::insert_target(sql, &opts()).unwrap().unwrap();
    assert_eq!(it.target, Target::In("system".into()));
    assert!(
        grammar::insert_target("INSERT INTO t (a) VALUES (1)", &opts())
            .unwrap()
            .is_none()
    );
    assert!(
        grammar::insert_target("SELECT 1", &opts())
            .unwrap()
            .is_none()
    );
}

#[test]
fn alter_and_show() {
    let p = |s: &str| grammar::statement(s, &opts()).unwrap().unwrap();
    assert_eq!(
        p("ALTER INHERITED STREAM security.user_events SPRIG SET (stream.retention_hours = 72)"),
        Stmt::Alter(grammar::AlterInherited {
            kind: DefKind::Stream,
            db: Some("security".into()),
            name: "user_events".into(),
            target: For::Own,
            op: AlterOp::Set(vec![("stream.retention_hours".into(), "72".into())]),
        })
    );
    match p("ALTER INHERITED TRIGGER sync_users_appb FOR TENANT client_x DISABLE") {
        Stmt::Alter(a) => {
            assert_eq!(a.target, For::Tenant("client_x".into()));
            assert_eq!(a.op, AlterOp::Disable);
        }
        Stmt::Show(s) => panic!("{s:?}"),
    }
    match p("ALTER INHERITED TRIGGER g FOR SUBTREE ENABLE") {
        Stmt::Alter(a) => assert_eq!((a.target, a.op), (For::Subtree(None), AlterOp::Enable)),
        Stmt::Show(s) => panic!("{s:?}"),
    }
    match p("ALTER INHERITED TRIGGER g FOR SUBTREE acme RESET") {
        Stmt::Alter(a) => {
            assert_eq!(
                (a.target, a.op),
                (For::Subtree(Some("acme".into())), AlterOp::Reset(vec![]))
            );
        }
        Stmt::Show(s) => panic!("{s:?}"),
    }
    match p("ALTER INHERITED TRIGGER g REPLACE LANGUAGE javascript AS $$ x $$;") {
        Stmt::Alter(a) => assert_eq!(
            a.op,
            AlterOp::Replace("LANGUAGE javascript AS $$ x $$".into())
        ),
        Stmt::Show(s) => panic!("{s:?}"),
    }
    match p("ALTER INHERITED STREAM s RESET (stream.retention_hours)") {
        Stmt::Alter(a) => assert_eq!(a.op, AlterOp::Reset(vec!["stream.retention_hours".into()])),
        Stmt::Show(s) => panic!("{s:?}"),
    }
    assert_eq!(
        p("SHOW INHERITED DEFINITIONS"),
        Stmt::Show(ShowInherited::Definitions)
    );
    assert_eq!(
        p("show inherited overrides for trigger app.g"),
        Stmt::Show(ShowInherited::Overrides(Some((
            DefKind::Trigger,
            Some("app".into()),
            "g".into()
        ))))
    );
    assert!(
        grammar::statement("ALTER TABLE t ADD c INT", &opts())
            .unwrap()
            .is_none()
    );
    assert!(grammar::statement("ALTER INHERITED STREAM s FROB", &opts()).is_err());
    assert_eq!(
        grammar::ddl_kind("CREATE OR REPLACE TRIGGER g ON STREAM s", &opts()),
        Some((DefKind::Trigger, false))
    );
    assert_eq!(
        grammar::ddl_kind("DROP STREAM IF EXISTS s", &opts()),
        Some((DefKind::Stream, true))
    );
    assert_eq!(grammar::ddl_kind("DROP TABLE s", &opts()), None);
}

#[test]
fn times_and_schemas() {
    assert_eq!(datetime(0), "1970-01-01 00:00:00.000");
    assert_eq!(datetime(1_762_646_400_123), "2025-11-09 00:00:00.123");
    assert_eq!(tenant_of_schema("t2430600001_sprig"), t(2_430_600_001));
    assert_eq!(tenant_of_schema("mysql"), t(0));
}

#[test]
fn spec_round_trips() {
    let s = StreamSpec {
        key: vec!["user_id".into()],
        partitions: 6,
        retention_hours: Some(168),
        cleanup: "delete".into(),
        settings: BTreeMap::new(),
    };
    let v = serde_json::to_value(&s).unwrap();
    assert_eq!(serde_json::from_value::<StreamSpec>(v).unwrap(), s);
    let g = vec![GrantItem {
        privileges: vec!["INSERT".into()],
        db: "app".into(),
        name: "users".into(),
    }];
    assert_eq!(grant_items(Some(&serde_json::to_value(&g).unwrap())), g);
    assert!(grant_items(None).is_empty());
}

#[test]
fn exchange_queue_and_binding_statements() {
    use grammar::MqDecl;
    let sql = "CREATE EXCHANGE jobs TYPE topic FOR EACH TENANT";
    let f = grammar::for_each(sql, &opts()).unwrap().unwrap();
    assert_eq!(
        grammar::mq_decl(&f.strip(sql), &opts()).unwrap(),
        Some(MqDecl::Exchange {
            if_not_exists: false,
            db: None,
            name: "jobs".into(),
            kind: "topic".into(),
            internal: false,
            arguments: Vec::new(),
        })
    );
    let sql = "CREATE QUEUE mq.welcome_mail FOR EACH TENANT UNDER acme \
               WITH (x_queue_type = 'quorum', x_message_ttl = 60000, x_flag = TRUE)";
    let f = grammar::for_each(sql, &opts()).unwrap().unwrap();
    assert_eq!(f.under.as_deref(), Some("acme"));
    assert_eq!(
        grammar::mq_decl(&f.strip(sql), &opts()).unwrap(),
        Some(MqDecl::Queue {
            if_not_exists: false,
            db: Some("mq".into()),
            name: "welcome_mail".into(),
            arguments: vec![
                ("x-queue-type".into(), json!("quorum")),
                ("x-message-ttl".into(), json!(60000)),
                ("x-flag".into(), json!(true)),
            ],
        })
    );
    let sql = "BIND QUEUE welcome_mail TO EXCHANGE jobs KEY 'mail.welcome' FOR EACH TENANT";
    let f = grammar::for_each(sql, &opts()).unwrap().unwrap();
    assert_eq!(
        grammar::mq_decl(&f.strip(sql), &opts()).unwrap(),
        Some(MqDecl::Bind {
            bind: true,
            db: None,
            dest_kind: "queue".into(),
            dest: "welcome_mail".into(),
            source: "jobs".into(),
            key: "mail.welcome".into(),
            arguments: Vec::new(),
        })
    );
    assert!(matches!(
        grammar::mq_decl("UNBIND EXCHANGE a FROM EXCHANGE b", &opts()).unwrap(),
        Some(MqDecl::Bind { bind: false, ref dest_kind, .. }) if dest_kind == "exchange"
    ));
    assert!(matches!(
        grammar::mq_decl("CREATE EXCHANGE IF NOT EXISTS e 'x-delayed-message' INTERNAL", &opts())
            .unwrap(),
        Some(MqDecl::Exchange { if_not_exists: true, internal: true, ref kind, .. })
            if kind == "x-delayed-message"
    ));
    assert_eq!(
        grammar::mq_decl("CREATE TABLE t (a INT)", &opts()).unwrap(),
        None
    );
    assert!(grammar::mq_decl("BIND QUEUE q INTO EXCHANGE e", &opts()).is_err());
    assert!(grammar::mq_decl("CREATE QUEUE q WITH (a 1)", &opts()).is_err());
    let b = BindingSpec {
        source: "jobs".into(),
        dest: "welcome_mail".into(),
        dest_kind: "queue".into(),
        key: "mail.welcome".into(),
        arguments: BTreeMap::new(),
    };
    assert_eq!(b.def_name(), "jobs>queue:welcome_mail:mail.welcome");
    let v = serde_json::to_value(&b).unwrap();
    assert_eq!(serde_json::from_value::<BindingSpec>(v).unwrap(), b);
}

#[test]
fn live_definitions_keep_their_database() {
    let s = def(1, 10, DefKind::Stream, "user_events", Scope::default());
    let g = def(1, 11, DefKind::Trigger, "sync_users", Scope::default());
    let mut dropping = (*def(1, 12, DefKind::Queue, "jobs", Scope::default())).clone();
    dropping.state = DefState::Dropping;
    let mut other = (*def(1, 13, DefKind::Exchange, "x", Scope::default())).clone();
    other.db_name = "billing".into();
    let st = state_with(&[g, s, Arc::new(dropping), Arc::new(other)], Vec::new());
    let live: Vec<String> = st
        .live_defs_in_db("SECURITY")
        .iter()
        .map(|d| format!("{} {}", d.kind.as_str(), d.name))
        .collect();
    assert_eq!(live, ["stream user_events", "trigger sync_users"]);
    assert!(st.live_defs_in_db("app").is_empty());
    assert_eq!(st.live_defs_in_db("billing").len(), 1);
}
