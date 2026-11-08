//! `sprigctl index` and `sprigctl explain query`: parsing, golden output,
//! and every route over HTTP against an in-process Admin API with a fake
//! service (the real one is tested in `sprig-controller::autoindex_admin`,
//! live against MariaDB).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use sprig_advisor::autoindex::model::{AutoIndexId, Evidence, KeyPart, PartSource, Role};
use sprig_advisor::autoindex::policy::{ObjectPolicy, SettingSource};
use sprig_advisor::autoindex::store::{
    AutoIndexRow, Baseline, CandidateRow, CandidateStatus, Definition, IndexState, Model,
    ShardBuild, StoredKind, ValidationMethod, ValidationRow, Verdict,
};
use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_common::{SprigCode, SprigError};
use sprig_controller::autoindex_admin::view::{
    self as v, Action, IndexActionRequest, IndexActionResponse, IndexDetail, ListIndexesResponse,
    ObjectNames, QueryExplanation,
};
use sprig_controller::autoindex_admin::{AutoIndexAdmin, ListIndexesQuery};
use sprig_settings::{MemSettings, SettingCtx};

use super::*;
use crate::index::{action_of, explain_text, index_list, index_path, index_show, list_path};

fn matches(args: &[&str]) -> clap::ArgMatches {
    let mut full = vec!["sprigctl"];
    full.extend_from_slice(args);
    cli().try_get_matches_from(full).unwrap()
}

fn sub_of(m: &clap::ArgMatches) -> (&str, &clap::ArgMatches) {
    let (_, g) = m.subcommand().unwrap();
    g.subcommand().unwrap()
}

const T0: u64 = 1_793_000_000;

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(T0 + secs)
}

const FP: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);

fn names() -> ObjectNames {
    ObjectNames(BTreeMap::from([
        (ObjectId(1), ("shop".to_owned(), "orders".to_owned())),
        (ObjectId(2), ("shop".to_owned(), "events".to_owned())),
    ]))
}

fn row(id: u64, object: u64, name: &str, state: IndexState) -> AutoIndexRow {
    AutoIndexRow {
        index_id: AutoIndexId(id),
        object: ObjectId(object),
        name: name.to_owned(),
        model: Model::Sql,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: vec![
                KeyPart::col("customer_id"),
                KeyPart::col("created_at").desc(),
            ],
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        state,
        prior_state: None,
        candidate_id: 11,
        validation_id: Some(111),
        decision_id: None,
        est_benefit_ms_h: 190_500.0,
        est_write_ms_h: 7_200.0,
        est_bytes: 2_405_181_685,
        actual_bytes: None,
        shards: BTreeMap::from([("17".to_owned(), ShardBuild::Built)]),
        baseline: BTreeMap::from([(
            FP.to_string(),
            Baseline {
                runs: 18_240,
                p95_ms: 412.0,
                mean_ms: 260.0,
            },
        )]),
        observed: None,
        last_used_at: None,
        drop_reason: None,
        created_at: at(3600),
        state_changed_at: at(7200),
    }
}

fn candidate() -> CandidateRow {
    CandidateRow {
        candidate_id: 11,
        run_id: 7,
        object: ObjectId(1),
        def_hash: 1,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: vec![
                KeyPart::col("customer_id"),
                KeyPart::col("created_at").desc(),
            ],
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        covering: false,
        fingerprints: vec![(FP, Role::Primary)],
        status: CandidateStatus::Recommended,
        reason: None,
        benefit_ms_h: Some(190_500.0),
        write_ms_h: Some(7_200.0),
        est_bytes: Some(2_405_181_685),
        build_seconds: Some(112),
        evidence: Evidence {
            parts: vec![PartSource::Eq, PartSource::Order],
            selectivity: BTreeMap::from([("customer_id".to_owned(), 8.3e-6)]),
            sel_source: BTreeMap::from([("customer_id".to_owned(), "avg_frequency".to_owned())]),
            entries_per_run: Some(858.0),
            notes: vec!["variant: eq + order, LIMIT".into()],
            ..Evidence::default()
        },
        created_at: at(0),
    }
}

fn validation() -> ValidationRow {
    ValidationRow {
        validation_id: 111,
        candidate_id: 11,
        method: ValidationMethod::Replica,
        cluster_id: "mum1-c7".into(),
        node: "n2".into(),
        shard_id: 17,
        started_at: at(1200),
        finished_at: Some(at(2400)),
        build_seconds: Some(118),
        statements: 260,
        noisy: 3,
        target_p95_before: Some(405.0),
        target_p95_after: Some(11.2),
        worst_regression: Some(3.1),
        worst_fp: Some(Fingerprint(0x51aa_02c4_e9d0_f337)),
        recall_at_k: None,
        verdict: Verdict::Accepted,
        reason: None,
        details: serde_json::json!({"samples": [1, 2, 3]}),
    }
}

fn list() -> ListIndexesResponse {
    let n = names();
    let mut dropped = row(3, 2, "sprig_auto_0badc0de", IndexState::Dropped);
    dropped.drop_reason = Some(sprig_advisor::autoindex::store::DropReason::NoGain);
    dropped.definition.parts = vec![KeyPart::col("account_id"), KeyPart::col("kind")];
    dropped.est_benefit_ms_h = 900.0;
    dropped.est_write_ms_h = 40.5;
    dropped.actual_bytes = Some(73_400_320);
    ListIndexesResponse {
        tenant: "acme".into(),
        indexes: vec![
            v::index_view(
                &row(1, 1, "sprig_auto_5be0c2a1", IndexState::Active),
                &n,
                None,
            ),
            v::index_view(
                &row(2, 1, "sprig_auto_1f00d5aa", IndexState::Recommended),
                &n,
                None,
            ),
            v::index_view(&dropped, &n, None),
        ],
    }
}

fn detail() -> IndexDetail {
    let n = names();
    let r = row(1, 1, "sprig_auto_5be0c2a1", IndexState::Active);
    let c = candidate();
    let val = [validation()];
    IndexDetail {
        tenant: "acme".into(),
        index: v::index_view(&r, &n, Some(&c)),
        candidate: Some(v::candidate_view(&c, &n)),
        validations: val.iter().map(v::validation_view).collect(),
        history: v::history(&r, Some(&c), &val),
    }
}

fn policy() -> ObjectPolicy {
    let mem = MemSettings::new(Registry::global().unwrap());
    let dc = DcId::new("mum1").unwrap();
    ObjectPolicy::resolve(&mem as &dyn SettingSource, &SettingCtx::process(&dc)).unwrap()
}

fn explanation() -> QueryExplanation {
    let mut skip = CandidateRow::skipped(
        7,
        ObjectId(2),
        FP,
        "function:LOWER",
        vec![
            "events.source uses a case-insensitive collation; WHERE source = ? can use an index"
                .into(),
        ],
        at(0),
    );
    skip.candidate_id = 12;
    let mut low = candidate();
    low.candidate_id = 9;
    low.run_id = 5;
    low.status = CandidateStatus::Rejected;
    low.reason = Some("below_benefit_ratio".into());
    low.benefit_ms_h = Some(1_650.0);
    low.write_ms_h = Some(40_000.0);
    let cands = vec![candidate(), skip, low];
    let mut ev = v::Evidence {
        candidates: &cands,
        ..v::Evidence::default()
    };
    ev.validations.insert(11, vec![validation()]);
    ev.indexes.insert(
        11,
        vec![row(2, 1, "sprig_auto_1f00d5aa", IndexState::Recommended)],
    );
    ev.policies.insert(ObjectId(1), policy());
    v::explain("acme", FP, at(0), &ev, &names())
}

#[test]
fn index_commands_parse() {
    let m = matches(&[
        "index",
        "list",
        "--tenant",
        "acme",
        "--db",
        "shop",
        "--table",
        "my orders",
        "--all",
    ]);
    let (_, sub) = sub_of(&m);
    assert_eq!(
        list_path(sub, false),
        "/admin/v1/tenants/acme/indexes?db=shop&table=my%20orders&all=true"
    );
    let m = matches(&["index", "list", "--tenant", "acme", "--recommended"]);
    assert_eq!(
        list_path(sub_of(&m).1, false),
        "/admin/v1/tenants/acme/recommendations"
    );
    let m = matches(&["index", "list", "--tenant", "acme", "--state", "frozen"]);
    assert_eq!(
        list_path(sub_of(&m).1, false),
        "/admin/v1/tenants/acme/indexes?state=frozen"
    );
    assert!(
        cli()
            .try_get_matches_from([
                "sprigctl",
                "index",
                "list",
                "--tenant",
                "a",
                "--recommended",
                "--state",
                "x"
            ])
            .is_err()
    );
    let m = matches(&[
        "index",
        "recommendations",
        "--tenant",
        "acme",
        "--db",
        "shop",
    ]);
    assert_eq!(
        list_path(sub_of(&m).1, true),
        "/admin/v1/tenants/acme/recommendations?db=shop"
    );
    // Writes and their aliases.
    for (args, action) in [
        (vec!["index", "approve", "7", "--tenant", "acme"], "approve"),
        (vec!["index", "apply", "7", "--tenant", "acme"], "approve"),
        (vec!["index", "dismiss", "7", "--tenant", "acme"], "reject"),
        (
            vec![
                "index",
                "drop",
                "7",
                "--tenant",
                "acme",
                "--reason",
                "slows writes",
            ],
            "drop",
        ),
        (vec!["index", "freeze", "7", "--tenant", "acme"], "pin"),
        (
            vec!["index", "freeze", "7", "--tenant", "acme", "--off"],
            "unpin",
        ),
        (vec!["index", "unpin", "7", "--tenant", "acme"], "unpin"),
    ] {
        let m = matches(&args);
        let (cmd, sub) = sub_of(&m);
        assert_eq!(action_of(cmd, sub), action, "{args:?}");
    }
    assert_eq!(
        index_path("acme", "shop.orders.sprig_auto_5be0c2a1", ":approve"),
        "/admin/v1/tenants/acme/indexes/shop.orders.sprig_auto_5be0c2a1:approve"
    );
    // --tenant is required (or SPRIGCTL_TENANT).
    if std::env::var_os("SPRIGCTL_TENANT").is_none() {
        assert!(
            cli()
                .try_get_matches_from(["sprigctl", "index", "list"])
                .is_err()
        );
    }
    let m = matches(&["explain", "query", "9c41d2e07b3a5f18", "--tenant", "acme"]);
    let (cmd, sub) = sub_of(&m);
    assert_eq!(cmd, "query");
    assert_eq!(
        sub.get_one::<String>("fingerprint").unwrap(),
        "9c41d2e07b3a5f18"
    );
    // Chapter 11's explain calls (crate::placement).
    let m = matches(&["explain", "decision", "17", "--tenant", "acme"]);
    let (cmd, sub) = sub_of(&m);
    assert_eq!(
        (cmd, sub.get_one::<String>("id").unwrap().as_str()),
        ("decision", "17")
    );
    let m = matches(&["explain", "placement", "acme"]);
    let (cmd, sub) = sub_of(&m);
    assert_eq!(
        (cmd, sub.get_one::<String>("tenant").unwrap().as_str()),
        ("placement", "acme")
    );
}

#[test]
fn golden_index_output() {
    golden("index-list.txt", &index_list(&list(), false));
    let empty = ListIndexesResponse {
        tenant: "acme".into(),
        indexes: Vec::new(),
    };
    assert_eq!(
        index_list(&empty, true),
        "no recommendations for tenant acme\n"
    );
    golden("index-show.txt", &index_show(&detail()));
    let approved = IndexActionResponse {
        tenant: "acme".into(),
        action: "approve".into(),
        changed: true,
        from: "recommended".into(),
        to: "rolling_out".into(),
        message: "handed to rollout: it is built shard by shard in the build window, then observed"
            .into(),
        index: list().indexes[1].clone(),
    };
    golden("index-approve.txt", &crate::index::action_text(&approved));
    let noop = IndexActionResponse {
        changed: false,
        message: "sprig_auto_5be0c2a1 is already pinned".into(),
        ..approved
    };
    assert_eq!(
        crate::index::action_text(&noop),
        "nothing to do: sprig_auto_5be0c2a1 is already pinned\n"
    );
    golden("explain-query.txt", &explain_text(&explanation()));
    let none = v::explain("acme", FP, at(0), &v::Evidence::default(), &names());
    golden("explain-query-none.txt", &explain_text(&none));
    // JSON never carries validation sample details.
    let j = serde_json::to_string(&detail()).unwrap();
    assert!(!j.contains("samples"), "{j}");
}

/// Answers from memory and remembers the actions.
#[derive(Default)]
struct FakeIndexes {
    acts: Mutex<Vec<(String, String, Action, IndexActionRequest)>>,
}

#[async_trait::async_trait]
impl AutoIndexAdmin for FakeIndexes {
    async fn list(
        &self,
        tenant: &str,
        q: &ListIndexesQuery,
    ) -> sprig_common::Result<ListIndexesResponse> {
        let mut r = list();
        tenant.clone_into(&mut r.tenant);
        r.indexes
            .retain(|i| q.state.as_deref().is_none_or(|s| i.state == s));
        if q.all != Some(true) {
            r.indexes.retain(|i| i.state != "dropped");
        }
        Ok(r)
    }

    async fn show(&self, _tenant: &str, index: &str) -> sprig_common::Result<IndexDetail> {
        if index == "1" || index == "shop.orders.sprig_auto_5be0c2a1" {
            return Ok(detail());
        }
        Err(SprigError::new(
            SprigCode::AutoIndexNotFound,
            format!("no automatic index {index:?}"),
        ))
    }

    async fn act(
        &self,
        tenant: &str,
        index: &str,
        action: Action,
        req: &IndexActionRequest,
    ) -> sprig_common::Result<IndexActionResponse> {
        if action == Action::Drop && index == "2" {
            return Err(SprigError::new(
                SprigCode::AutoIndexStateInvalid,
                "sprig_auto_1f00d5aa is recommended: it isn't built; reject the recommendation instead",
            ));
        }
        self.acts
            .lock()
            .unwrap()
            .push((tenant.to_owned(), index.to_owned(), action, req.clone()));
        Ok(IndexActionResponse {
            tenant: tenant.to_owned(),
            action: action.as_str().to_owned(),
            changed: true,
            from: "recommended".into(),
            to: "rolling_out".into(),
            message: "handed to rollout".into(),
            index: list().indexes[1].clone(),
        })
    }

    async fn explain_query(
        &self,
        _tenant: &str,
        fingerprint: &str,
    ) -> sprig_common::Result<QueryExplanation> {
        if fingerprint.len() != 16 {
            return Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                "not 16 hex digits",
            ));
        }
        Ok(explanation())
    }
}

#[tokio::test]
async fn index_calls_over_the_admin_api() {
    let store = MemStore::new(Tier::Dc(DcId::new("ctl-t").unwrap()));
    let global = store.with_tier(Tier::Global);
    let live = LiveSettings::start(
        Registry::global().unwrap(),
        SettingsStores::new(Arc::new(global.clone()), Arc::new(store.clone())).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let registry = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(store.clone())).unwrap();
    let monitor = Arc::new(ClusterMonitor::new(registry, live, Arc::new(SystemClock)));
    let (_tx, rx) = tokio::sync::watch::channel(Status::default());
    let stores = Stores {
        dc: Arc::new(store.clone()),
        global: Arc::new(global.clone()),
    };
    let fake = Arc::new(FakeIndexes::default());
    let svc = Arc::new(
        AdminService::new(
            stores.clone(),
            Arc::clone(&monitor),
            rx.clone(),
            Arc::new(SystemClock),
        )
        .with_indexes(Arc::clone(&fake) as Arc<dyn AutoIndexAdmin>),
    );
    let bare = Arc::new(AdminService::new(
        stores,
        monitor,
        rx,
        Arc::new(SystemClock),
    ));
    let stop = sprig_common::CancellationToken::new();
    let mut ends = Vec::new();
    for s in [svc, bare] {
        let l = http::bind("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        ends.push(format!("http://{}", l.local_addr().unwrap()));
        tokio::spawn(http::serve(l, http::router(s, None), stop.clone()));
    }
    let c = AdminClient::new(&ends[0], None).unwrap();

    let l: ListIndexesResponse = c.get("/admin/v1/tenants/acme/indexes").await.unwrap();
    assert_eq!(l.indexes.len(), 2);
    let l: ListIndexesResponse = c
        .get("/admin/v1/tenants/acme/indexes?all=true")
        .await
        .unwrap();
    assert_eq!(l.indexes.len(), 3);
    let l: ListIndexesResponse = c
        .get("/admin/v1/tenants/acme/recommendations")
        .await
        .unwrap();
    assert_eq!(
        l.indexes
            .iter()
            .map(|i| i.name.as_str())
            .collect::<Vec<_>>(),
        ["sprig_auto_1f00d5aa"]
    );
    let e = c
        .get::<ListIndexesResponse>("/admin/v1/tenants/acme/indexes?all=perhaps")
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");

    let d: IndexDetail = c
        .get("/admin/v1/tenants/acme/indexes/shop.orders.sprig_auto_5be0c2a1")
        .await
        .unwrap();
    assert_eq!(d.index.index_id, 1);
    let e = c
        .get::<IndexDetail>("/admin/v1/tenants/acme/indexes/99")
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .starts_with("39201 ER_SPRIG_AUTO_INDEX_NOT_FOUND"),
        "{e}"
    );

    // Actions, chapter 13's spellings included.
    for (m, a) in [
        ("approve", Action::Approve),
        ("apply", Action::Approve),
        ("reject", Action::Reject),
        ("pin", Action::Pin),
        ("freeze", Action::Pin),
        ("unpin", Action::Unpin),
    ] {
        let r: IndexActionResponse = c
            .post(
                &format!("/admin/v1/tenants/acme/indexes/2:{m}"),
                &IndexActionRequest {
                    reason: "ok".into(),
                    actor: Some("kiran".into()),
                },
            )
            .await
            .unwrap();
        assert!(r.changed);
        assert_eq!(fake.acts.lock().unwrap().last().unwrap().2, a);
    }
    let e = c
        .post::<_, IndexActionResponse>(
            "/admin/v1/tenants/acme/indexes/2:drop",
            &IndexActionRequest::default(),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39202"), "{e}");
    let e = c
        .post::<_, IndexActionResponse>(
            "/admin/v1/tenants/acme/indexes/2:build",
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains(":approve"), "{e}");
    let e = c
        .post::<_, IndexActionResponse>(
            "/admin/v1/tenants/acme/indexes/2:approve",
            &serde_json::json!({"why": "x"}),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");

    let x: QueryExplanation = c
        .get("/admin/v1/tenants/acme/queries/9c41d2e07b3a5f18:explain")
        .await
        .unwrap();
    assert_eq!(x.fingerprint, "9c41d2e07b3a5f18");
    let e = c
        .get::<QueryExplanation>("/admin/v1/tenants/acme/queries/9c41d2e07b3a5f18")
        .await
        .unwrap_err();
    assert!(e.to_string().contains(":explain"), "{e}");

    // A controller without the service says so.
    let bare = AdminClient::new(&ends[1], None).unwrap();
    let e = bare
        .get::<ListIndexesResponse>("/admin/v1/tenants/acme/indexes")
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("doesn't serve automatic indexes"),
        "{e}"
    );
    stop.cancel();
}
