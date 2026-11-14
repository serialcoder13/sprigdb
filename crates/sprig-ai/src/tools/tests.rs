use serde_json::json;

use super::*;

#[test]
fn builtin_tools_are_valid_and_split_by_role() {
    let r = ToolRegistry::builtin();
    assert_eq!(r.all().count(), builtin::specs().len());
    assert!(r.for_role(Role::Triage).is_empty());
    let names = |role| -> Vec<&str> { r.for_role(role).iter().map(|s| s.name).collect() };
    assert!(names(Role::Data).contains(&"run_query"));
    assert!(!names(Role::Dba).contains(&"run_query"));
    assert!(names(Role::Admin).contains(&"metrics_trend"));
    assert!(!names(Role::Admin).contains(&"describe"));
    assert!(!names(Role::Admin).contains(&"propose_sql"));
    for s in r.all() {
        // Every proposal tool maps to a proposal kind, and only those.
        assert_eq!(
            builtin::proposal_kind(s.name).is_some(),
            s.effect == Effect::Propose,
            "{}",
            s.name
        );
        assert!(s.description.len() > 20, "{}", s.name);
        let def = s.def();
        assert!(def.strict);
        assert_eq!(def.input_schema["type"], "object");
    }
}

#[test]
fn calls_are_checked_before_anything_runs() {
    let r = ToolRegistry::builtin();
    let ok = r.check(
        Role::Data,
        "run_query",
        &json!({"db": "shop", "sql": "SELECT 1"}),
    );
    assert!(ok.is_ok());
    let deny = |role, name, input| r.check(role, name, &input).unwrap_err();
    let e = deny(
        Role::Dba,
        "run_query",
        json!({"db": "shop", "sql": "SELECT 1"}),
    );
    assert!(e.denied && e.message.contains("may not call"));
    let e = deny(Role::Data, "nope", json!({}));
    assert!(e.message.contains("no tool"));
    let e = deny(Role::Data, "run_query", json!({"db": "shop"}));
    assert!(e.message.contains("sql is required"));
    let e = deny(
        Role::Data,
        "run_query",
        json!({"db": "shop", "sql": "DELETE FROM t"}),
    );
    assert!(e.message.contains("propose_sql"), "{}", e.message);
    let e = deny(
        Role::Data,
        "run_query",
        json!({"db": "s", "sql": "SELECT 1", "limit": 5000}),
    );
    assert!(e.message.contains("limit"));
    let e = deny(
        Role::Dba,
        "explain",
        json!({"db": "s", "sql": "EXPLAIN ANALYZE SELECT 1"}),
    );
    assert!(e.denied);
    let e = deny(Role::Dba, "workload", json!({"hours": 0}));
    assert!(e.message.contains("hours"));
    let e = deny(
        Role::Data,
        "propose_tile",
        json!({"title": "t", "sql": "SELECT 1", "chart": "{\"mark\":\"spiral\"}"}),
    );
    assert!(e.message.contains("unknown mark"));
    let e = deny(
        Role::Data,
        "describe",
        json!({"db": "s", "table": "t", "extra": 1}),
    );
    assert!(e.message.contains("unknown field extra"));
    assert!(
        r.check(
            Role::Data,
            "propose_tile",
            &json!({"title": "t", "sql": "SELECT 1", "chart": "{\"mark\":\"bar\",\"columns\":[]}"})
        )
        .is_ok()
    );
}

#[test]
fn proposals_never_reach_a_host() {
    let r = ToolRegistry::builtin();
    let input = json!({"db": "shop", "sql": "DROP TABLE orders", "why": "unused"});
    let spec = r.check(Role::Dba, "propose_sql", &input).unwrap();
    let p = ToolRegistry::propose(&spec, "toolu_1", &input).unwrap();
    assert_eq!(p.kind, ProposalKind::Sql);
    assert_eq!(p.body, input);
    assert_eq!(p.tool_use_id, "toolu_1");
    let note = ToolRegistry::proposal_note(&p);
    assert!(note["status"].as_str().unwrap().contains("not run"));
    // A read tool can't be turned into a proposal.
    let read = r.get("describe").unwrap();
    assert!(ToolRegistry::propose(read, "x", &json!({})).is_err());
}

#[test]
fn registries_refuse_bad_specs() {
    let mut s = builtin::specs();
    s.push(s[0].clone());
    assert!(ToolRegistry::new(s).is_err());
    let bad = ToolSpec {
        name: "open",
        description: "an open schema",
        input: json!({"type": "object"}),
        effect: Effect::Read,
        roles: &[Role::Data],
        shares: Shares::Schema,
    };
    assert!(ToolRegistry::new([bad]).is_err());
}
