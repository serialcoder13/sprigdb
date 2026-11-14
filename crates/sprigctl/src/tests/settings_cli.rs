//! `sprigctl settings` and `tenant settings`: parsing, how flags become a
//! scope and a context, golden output, and every route over HTTP against
//! an in-process Admin API with a fake settings service (the real service
//! is tested in `sprig-controller::settings_admin`).

use std::sync::Mutex;

use sprig_common::{SprigCode, SprigError};
use sprig_controller::admin::types::{
    ListSettingKeysQuery, ListSettingKeysResponse, LockSettingRequest, ResetSettingRequest,
    ScopeRef, SetBoundsRequest, SetSettingRequest, SettingContextQuery, SettingContextView,
    SettingExplanation, SettingKeyView, SettingOp, SettingRowView, SettingStepView,
    SettingWarningView, SettingWrite, SettingWriteResponse, TenantSettingRowView,
    TenantSettingsResponse,
};
use sprig_controller::settings_admin::SettingsAdmin;
use sprig_settings::{Bounds, Value};

use super::*;
use crate::settings::{escape, get_query, scope_ref};

fn matches(args: &[&str]) -> clap::ArgMatches {
    let mut full = vec!["sprigctl"];
    full.extend_from_slice(args);
    cli().try_get_matches_from(full).unwrap()
}

/// The scope a write command line names.
fn scope_of(args: &[&str]) -> Result<String, String> {
    let m = matches(args);
    let (_, g) = m.subcommand().unwrap();
    let (_, sub) = g.subcommand().unwrap();
    scope_ref(&m, sub)
        .map(|s| s.to_string())
        .map_err(|Failed(e)| e)
}

fn query_of(args: &[&str]) -> String {
    let m = matches(args);
    let (_, g) = m.subcommand().unwrap();
    let (_, sub) = g.subcommand().unwrap();
    get_query(&m, sub).map_err(|Failed(e)| e).unwrap()
}

#[test]
fn settings_commands_parse() {
    for args in [
        &["settings", "list"][..],
        &[
            "settings",
            "list",
            "--prefix",
            "catalog.",
            "--operator-only",
        ],
        &[
            "settings",
            "list",
            "--tenant-settable",
            "--accepts",
            "object",
        ],
        &["settings", "describe", "auto_index.mode"],
        &[
            "settings", "get", "k.x", "--tenant", "acme", "--db", "shop", "--object", "orders",
        ],
        &[
            "settings",
            "explain",
            "k.x",
            "--scope",
            "object:acme.shop.orders",
        ],
        &[
            "config",
            "explain",
            "k.x",
            "--scope",
            "object:acme.shop.orders",
        ],
        &[
            "settings", "set", "k.x", "8", "--scope", "global", "--reason", "why",
        ],
        &["settings", "set", "k.x", "-1", "--scope", "dc"],
        &["config", "set", "k.x", "40", "--scope", "tenant:acme"],
        &[
            "settings", "reset", "k.x", "--scope", "tenant", "--tenant", "acme", "--bounds",
        ],
        &["config", "unset", "k.x", "--scope", "global", "--all"],
        &[
            "settings", "bounds", "k.x", "--scope", "global", "--min", "-1", "--max", "8",
        ],
        &[
            "settings",
            "bounds",
            "k.x",
            "--scope",
            "global",
            "--allowed",
            "on,off",
            "--clear",
            "min,max",
        ],
        &[
            "settings",
            "lock",
            "k.x",
            "--scope",
            "cluster",
            "--cluster",
            "mum1-c1",
        ],
        &["settings", "unlock", "k.x", "--scope", "global"],
        &["tenant", "settings", "acme", "--all"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(args);
        assert!(cli().try_get_matches_from(full).is_ok(), "{args:?}");
    }
    for bad in [
        &["settings", "set", "k.x", "8"][..],
        &["settings", "set", "k.x", "--scope", "global"],
        &[
            "settings", "reset", "k.x", "--scope", "global", "--bounds", "--all",
        ],
        &[
            "settings",
            "bounds",
            "k.x",
            "--scope",
            "global",
            "--clear",
            "everything",
        ],
        &[
            "settings", "bounds", "k.x", "--scope", "global", "--max", "lots",
        ],
        &["settings", "list", "--operator-only", "--tenant-settable"],
        &["settings", "get"],
        &["tenant", "settings"],
    ] {
        let mut full = vec!["sprigctl"];
        full.extend_from_slice(bad);
        assert!(cli().try_get_matches_from(full).is_err(), "{bad:?}");
    }
}

#[test]
fn flags_become_scopes_and_contexts() {
    let ok = |args: &[&str], want: &str| assert_eq!(scope_of(args).unwrap(), want, "{args:?}");
    ok(&["settings", "lock", "k.x", "--scope", "global"], "global");
    ok(&["settings", "lock", "k.x", "--scope", "dc"], "dc");
    ok(
        &["--dc", "mum1", "settings", "lock", "k.x", "--scope", "dc"],
        "dc:mum1",
    );
    ok(
        &[
            "settings",
            "lock",
            "k.x",
            "--scope",
            "cluster",
            "--cluster",
            "mum1-c7",
        ],
        "cluster:mum1-c7",
    );
    ok(
        &[
            "settings", "lock", "k.x", "--scope", "tenant", "--tenant", "acme",
        ],
        "tenant:acme",
    );
    ok(
        &[
            "settings", "lock", "k.x", "--scope", "database", "--tenant", "acme", "--db", "shop",
        ],
        "db:acme.shop",
    );
    ok(
        &[
            "settings", "lock", "k.x", "--scope", "object", "--tenant", "acme", "--db", "shop",
            "--object", "orders",
        ],
        "object:acme.shop.orders",
    );
    ok(
        &[
            "settings",
            "lock",
            "k.x",
            "--scope",
            "object",
            "--tenant",
            "acme",
            "--object",
            "shop.orders",
        ],
        "object:acme.shop.orders",
    );
    ok(
        &["settings", "lock", "k.x", "--scope", "object:a.b.c"],
        "object:a.b.c",
    );
    for (args, want) in [
        (
            &["settings", "lock", "k.x", "--scope", "tenant"][..],
            "needs --tenant",
        ),
        (
            &["settings", "lock", "k.x", "--scope", "cluster"],
            "needs --cluster",
        ),
        (
            &[
                "settings", "lock", "k.x", "--scope", "object", "--tenant", "a", "--object", "o",
            ],
            "needs --db",
        ),
        (
            &["settings", "lock", "k.x", "--scope", "planet"],
            "expected global",
        ),
        (
            &["settings", "lock", "k.x", "--scope", "db:acme"],
            "expected db:",
        ),
    ] {
        let e = scope_of(args).unwrap_err();
        assert!(e.contains(want), "{args:?}: {e}");
    }
    assert_eq!(query_of(&["settings", "get", "k.x"]), "");
    assert_eq!(
        query_of(&[
            "--dc",
            "mum1",
            "settings",
            "get",
            "k.x",
            "--tenant",
            "acme",
            "--db",
            "my shop",
            "--cluster",
            "mum1-c1"
        ]),
        "?tenant=acme&db=my%20shop&cluster=mum1-c1&dc=mum1"
    );
    assert_eq!(
        query_of(&[
            "settings",
            "get",
            "k.x",
            "--scope",
            "object:acme.shop.orders"
        ]),
        "?tenant=acme&db=shop&object=orders"
    );
    assert_eq!(escape("a.b-c_d~e/f&g"), "a.b-c_d~e%2Ff%26g");
}

fn key_view(key: &str, kind: &str, default: Value, op: bool) -> SettingKeyView {
    SettingKeyView {
        key: key.into(),
        kind: kind.into(),
        default,
        scopes: if op {
            vec!["global".into(), "dc".into()]
        } else {
            ["global", "dc", "cluster", "tenant", "database", "object"]
                .map(String::from)
                .to_vec()
        },
        hard_bounds: if kind == "int" {
            Bounds {
                min: Some(1.0),
                max: Some(64.0),
                ..Bounds::default()
            }
        } else {
            Bounds::default()
        },
        apply: "live".into(),
        operator_only: op,
        tenant_settable: !op,
        owner: if op { 8 } else { 13 },
        doc: if op {
            "Cache memory per gateway".into()
        } else {
            "Automatic indexes per table".into()
        },
    }
}

fn hard(max: f64) -> Bounds {
    Bounds {
        min: Some(1.0),
        max: Some(max),
        ..Bounds::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn step(
    scope: &str,
    target: Option<&str>,
    value: Option<i64>,
    bounds: Option<Bounds>,
    in_force: Bounds,
    outcome: &str,
    set_by: Option<&str>,
) -> SettingStepView {
    SettingStepView {
        scope: scope.into(),
        target: target.map(Into::into),
        accepted: outcome != "not_accepted",
        value: value.map(Value::Int),
        bounds,
        bounds_in_force: in_force,
        outcome: outcome.into(),
        clamped_to: (outcome == "effective").then_some(Value::Int(5)),
        set_by: set_by.map(Into::into),
        set_at: set_by.map(|_| "2026-11-08T09:30:00Z".into()),
        reason: set_by.map(|_| "capacity".into()),
        version: None,
    }
}

/// An example explanation, as the API answers it.
fn explanation() -> SettingExplanation {
    SettingExplanation {
        key: "auto_index.max_per_table".into(),
        declaration: key_view("auto_index.max_per_table", "int", Value::Int(5), false),
        context: SettingContextView {
            dc: sprig_common::ids::DcId::new("mum1").unwrap(),
            cluster: None,
            tenant: Some("acme".into()),
            db: Some("shop".into()),
            object: Some("orders".into()),
        },
        value: Value::Int(5),
        source: "object".into(),
        source_target: Some("acme.shop.orders".into()),
        clamped: true,
        bounds: hard(5.0),
        chain: vec![
            step(
                "default",
                None,
                Some(5),
                None,
                hard(64.0),
                "overridden",
                None,
            ),
            step(
                "global",
                None,
                None,
                Some(Bounds::max(8.0)),
                hard(64.0),
                "bounds_only",
                Some("op:kiran"),
            ),
            step("dc", Some("mum1"), None, None, hard(8.0), "unset", None),
            step(
                "tenant",
                Some("acme"),
                Some(6),
                Some(Bounds::max(5.0)),
                hard(8.0),
                "overridden",
                Some("op:kiran"),
            ),
            step(
                "database",
                Some("acme.shop"),
                None,
                None,
                hard(5.0),
                "unset",
                None,
            ),
            step(
                "object",
                Some("acme.shop.orders"),
                Some(7),
                None,
                hard(5.0),
                "effective",
                Some("user:7"),
            ),
        ],
        catalog_version: Some(12),
    }
}

fn write_response() -> SettingWriteResponse {
    SettingWriteResponse {
        key: "auto_index.max_per_table".into(),
        scope: "tenant:acme".parse().unwrap(),
        action: "bounds".into(),
        changed: true,
        version: Some(13),
        previous: Some(SettingRowView {
            value: Some(Value::Int(6)),
            ..SettingRowView::default()
        }),
        current: Some(SettingRowView {
            value: Some(Value::Int(6)),
            bounds: Some(Bounds::max(5.0)),
            set_by: Some("op:kiran".into()),
            ..SettingRowView::default()
        }),
        warnings: vec![SettingWarningView {
            scope: "object:acme.shop.orders".into(),
            value: Value::Int(7),
            message: "auto_index.max_per_table: the value at object:acme.shop.orders is outside the bounds in force and is read as 5".into(),
        }],
        summary: Some("1 value will be clamped".into()),
        set_by: "op:kiran".into(),
    }
}

fn tenant_rows() -> TenantSettingsResponse {
    let row = |scope: &str, target: &str, key: &str, v: Option<Value>, b: Option<Bounds>| {
        TenantSettingRowView {
            scope: scope.into(),
            target: target.into(),
            key: key.into(),
            value: v,
            bounds: b,
            set_by: "op:kiran".into(),
            set_at: "2026-11-08T09:30:00Z".into(),
            version: 12,
        }
    };
    TenantSettingsResponse {
        tenant: "acme".into(),
        tenant_id: sprig_common::ids::TenantId(42),
        catalog_version: 13,
        rows: vec![
            row(
                "tenant",
                "acme",
                "auto_index.max_per_table",
                Some(Value::Int(6)),
                Some(Bounds::max(5.0)),
            ),
            row(
                "object",
                "acme.shop.orders",
                "auto_index.mode",
                Some(Value::from("on")),
                None,
            ),
            row(
                "ancestor",
                "platform",
                "auth.password_lock_minutes",
                Some(Value::Int(30)),
                None,
            ),
        ],
    }
}

#[test]
fn golden_settings_output() {
    let keys = vec![
        key_view("auto_index.max_per_table", "int", Value::Int(5), false),
        key_view("catalog.cache_max_mb", "int", Value::Int(512), true),
    ];
    golden("settings-list.txt", &render::settings_list(&keys));
    golden(
        "settings-describe.txt",
        &render::settings_describe(&keys[0]),
    );
    golden("settings-get.txt", &render::settings_get(&explanation()));
    golden(
        "settings-write.txt",
        &render::settings_write(&write_response()),
    );
    let mut same = write_response();
    same.changed = false;
    golden(
        "settings-write-unchanged.txt",
        &render::settings_write(&same),
    );
    golden(
        "tenant-settings.txt",
        &render::tenant_settings(&tenant_rows()),
    );
    let mut empty = tenant_rows();
    empty.rows.clear();
    assert!(render::tenant_settings(&empty).contains("every key inherits"));
}

/// Answers settings calls from memory and remembers the writes.
#[derive(Default)]
struct FakeSettings {
    writes: Mutex<Vec<(String, SettingWrite, usize)>>,
}

#[async_trait::async_trait]
impl SettingsAdmin for FakeSettings {
    async fn list_keys(
        &self,
        q: &ListSettingKeysQuery,
    ) -> sprig_common::Result<ListSettingKeysResponse> {
        let all = [
            key_view("auto_index.max_per_table", "int", Value::Int(5), false),
            key_view("catalog.cache_max_mb", "int", Value::Int(512), true),
        ];
        Ok(ListSettingKeysResponse {
            keys: all
                .into_iter()
                .filter(|k| q.prefix.as_deref().is_none_or(|p| k.key.starts_with(p)))
                .filter(|k| q.operator_only.is_none_or(|o| o != k.tenant_settable))
                .collect(),
        })
    }

    async fn explain(
        &self,
        key: &str,
        q: &SettingContextQuery,
    ) -> sprig_common::Result<SettingExplanation> {
        if key != "auto_index.max_per_table" {
            return Err(SprigError::new(
                SprigCode::SettingUnknown,
                format!("unknown setting {key}"),
            ));
        }
        let mut e = explanation();
        e.context.tenant.clone_from(&q.tenant);
        e.context.db.clone_from(&q.db);
        e.context.object.clone_from(&q.object);
        Ok(e)
    }

    async fn write(
        &self,
        key: &str,
        w: &SettingWrite,
        guard: Vec<sprig_meta::Cmp>,
    ) -> sprig_common::Result<SettingWriteResponse> {
        if key == "doc.wildcard_index" && w.scope.kind() == "cluster" {
            return Err(SprigError::new(
                SprigCode::SettingScopeNotAllowed,
                "doc.wildcard_index can't be set at cluster scope",
            ));
        }
        if matches!(&w.op, SettingOp::Set { value_text: Some(t), .. } if t == "99") {
            return Err(SprigError::new(
                SprigCode::SettingOutOfBounds,
                "auto_index.max_per_table: 99 is outside the bounds in force (min 1, max 64)",
            ));
        }
        self.writes
            .lock()
            .unwrap()
            .push((key.to_owned(), w.clone(), guard.len()));
        let mut r = write_response();
        r.key = key.to_owned();
        r.scope = w.scope.clone();
        r.action = w.op.action().to_owned();
        Ok(r)
    }

    async fn tenant_settings(&self, name: &str) -> sprig_common::Result<TenantSettingsResponse> {
        let mut r = tenant_rows();
        name.clone_into(&mut r.tenant);
        Ok(r)
    }
}

#[tokio::test]
async fn settings_calls_over_the_admin_api() {
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
    let (status_tx, rx) = tokio::sync::watch::channel(Status::default());
    let stores = Stores {
        dc: Arc::new(store.clone()),
        global: Arc::new(global.clone()),
    };
    let fake = Arc::new(FakeSettings::default());
    let svc = Arc::new(
        AdminService::new(
            stores.clone(),
            Arc::clone(&monitor),
            rx.clone(),
            Arc::new(SystemClock),
        )
        .with_settings(Arc::clone(&fake) as Arc<dyn SettingsAdmin>),
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
        let l = http::bind("127.0.0.1:0".parse().unwrap(), Some("tok"))
            .await
            .unwrap();
        ends.push(format!("http://{}", l.local_addr().unwrap()));
        tokio::spawn(http::serve(
            l,
            http::router(s, Some("tok".into())),
            stop.clone(),
        ));
    }
    let c = AdminClient::new(&ends[0], Some("tok".into())).unwrap();
    let k = "auto_index.max_per_table";

    // Reads.
    let l: ListSettingKeysResponse = c.get("/admin/v1/settings").await.unwrap();
    assert_eq!(l.keys.len(), 2);
    let l: ListSettingKeysResponse = c
        .get("/admin/v1/settings?prefix=catalog.&operator_only=true")
        .await
        .unwrap();
    assert_eq!(l.keys.len(), 1);
    let e = c
        .get::<ListSettingKeysResponse>("/admin/v1/settings?operator_only=perhaps")
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");
    let e = c
        .get::<ListSettingKeysResponse>("/admin/v1/settings?colour=red")
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");
    let x: SettingExplanation = c
        .get(&format!(
            "/admin/v1/settings/{k}?tenant=acme&db=my%20shop&object=orders"
        ))
        .await
        .unwrap();
    assert_eq!(x.context.db.as_deref(), Some("my shop"));
    let x2: SettingExplanation = c
        .get(&format!("/admin/v1/settings/{k}:explain?tenant=acme"))
        .await
        .unwrap();
    assert_eq!(x2.context.tenant.as_deref(), Some("acme"));
    assert!(render::settings_get(&x2).contains("◄ effective, clamped to 5"));
    let e = c
        .get::<SettingExplanation>("/admin/v1/settings/no.such_key")
        .await
        .unwrap_err();
    assert!(
        e.to_string().starts_with("39081 ER_SPRIG_SETTING_UNKNOWN"),
        "{e}"
    );
    let e = c
        .get::<SettingExplanation>(&format!("/admin/v1/settings/{k}:history"))
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");
    let t: TenantSettingsResponse = c.get("/admin/v1/tenants/acme/settings").await.unwrap();
    assert_eq!(t.rows.len(), 3);

    // An etcd-scope write needs the DC leader; a catalog-scope one doesn't.
    let set = |scope: &str, v: &str| SetSettingRequest {
        scope: Some(scope.parse().unwrap()),
        value: None,
        value_text: Some(v.into()),
        reason: "why".into(),
        actor: Some("kiran".into()),
    };
    let path = format!("/admin/v1/settings/{k}");
    let e = c
        .put::<_, SettingWriteResponse>(&path, &set("global", "8"))
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39504"), "{e}");
    let r: SettingWriteResponse = c.put(&path, &set("tenant:acme", "8")).await.unwrap();
    assert_eq!(r.scope, ScopeRef::Tenant("acme".into()));
    status_tx.send_modify(|s| s.dc_leader = Some(7));
    let r: SettingWriteResponse = c.put(&path, &set("global", "8")).await.unwrap();
    assert_eq!(r.action, "set");
    // With global leadership, global writes use that guard.
    status_tx.send_modify(|s| s.global_leader = Some(9));
    let _: SettingWriteResponse = c.put(&path, &set("global", "8")).await.unwrap();
    // Refusals pass through.
    let e = c
        .put::<_, SettingWriteResponse>(&path, &set("dc", "99"))
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .starts_with("39080 ER_SPRIG_SETTING_OUT_OF_BOUNDS"),
        "{e}"
    );
    let e = c
        .put::<_, SettingWriteResponse>(
            "/admin/v1/settings/doc.wildcard_index",
            &set("cluster:ctl-t-c1", "on"),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39082"), "{e}");
    let e = c
        .put::<_, SettingWriteResponse>(&path, &SetSettingRequest::default())
        .await
        .unwrap_err();
    assert!(e.to_string().contains("scope is required"), "{e}");
    let e = c
        .put::<_, SettingWriteResponse>(&path, &serde_json::json!({"scope": "planet:x"}))
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");

    // The other write routes.
    let _: SettingWriteResponse = c
        .delete(&format!("{path}?scope=db%3Aacme.shop&reason=gone"))
        .await
        .unwrap();
    let _: SettingWriteResponse = c
        .post(
            &format!("{path}:reset"),
            &ResetSettingRequest {
                scope: Some("tenant:acme".parse().unwrap()),
                bounds: true,
                ..ResetSettingRequest::default()
            },
        )
        .await
        .unwrap();
    let _: SettingWriteResponse = c
        .post(
            &format!("{path}:bounds"),
            &SetBoundsRequest {
                scope: Some("global".parse().unwrap()),
                max: Some(8.0),
                clear: vec!["min".into()],
                ..SetBoundsRequest::default()
            },
        )
        .await
        .unwrap();
    for m in ["lock", "unlock"] {
        let r: SettingWriteResponse = c
            .post(
                &format!("{path}:{m}"),
                &LockSettingRequest {
                    scope: Some("dc".parse().unwrap()),
                    ..LockSettingRequest::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.action, m);
    }
    let e = c
        .post::<_, SettingWriteResponse>(&format!("{path}:freeze"), &serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(e.to_string().starts_with("39792"), "{e}");
    let writes = fake.writes.lock().unwrap().clone();
    let summary: Vec<(String, String, usize)> = writes
        .iter()
        .map(|(_, w, g)| (w.scope.to_string(), w.op.action().to_owned(), *g))
        .collect();
    assert_eq!(
        summary,
        [
            ("tenant:acme".to_owned(), "set".to_owned(), 0),
            ("global".into(), "set".into(), 1),
            ("global".into(), "set".into(), 1),
            ("db:acme.shop".into(), "reset".into(), 0),
            ("tenant:acme".into(), "reset".into(), 0),
            ("global".into(), "bounds".into(), 1),
            ("dc".into(), "lock".into(), 1),
            ("dc".into(), "unlock".into(), 1),
        ]
    );
    assert_eq!(writes[0].1.actor, "kiran");
    assert_eq!(writes[0].1.reason, "why");
    assert_eq!(
        writes[3].1.op,
        SettingOp::Reset {
            value: true,
            bounds: false
        }
    );
    assert_eq!(writes[3].1.reason, "gone");
    assert_eq!(
        writes[4].1.op,
        SettingOp::Reset {
            value: false,
            bounds: true
        }
    );
    assert!(
        matches!(&writes[5].1.op, SettingOp::Bounds { max: Some(m), clear, .. } if (*m - 8.0).abs() < f64::EPSILON && clear == &["min"])
    );

    // The commands themselves, end to end over the client.
    for args in [
        &["settings", "list", "--operator-only"][..],
        &["settings", "describe", k],
        &["settings", "get", k, "--scope", "object:acme.shop.orders"],
        &[
            "settings", "set", k, "8", "--scope", "tenant", "--tenant", "acme",
        ],
        &[
            "settings", "bounds", k, "--scope", "dc", "--max", "8", "--clear", "min",
        ],
        &["settings", "reset", k, "--scope", "db:acme.shop", "--all"],
        &["settings", "unlock", k, "--scope", "global"],
    ] {
        let m = matches(args);
        let (_, g) = m.subcommand().unwrap();
        crate::settings::run(&c, &m, g, Output::Json).await.unwrap();
    }
    let m = matches(&["tenant", "settings", "acme"]);
    let (_, g) = m.subcommand().unwrap();
    let (_, sub) = g.subcommand().unwrap();
    crate::settings::tenant(&c, sub, Output::Table)
        .await
        .unwrap();
    let last = fake.writes.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.1.op, SettingOp::Lock { locked: false });

    // A controller without the settings service says so.
    let e = AdminClient::new(&ends[1], Some("tok".into()))
        .unwrap()
        .get::<ListSettingKeysResponse>("/admin/v1/settings")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("doesn't serve settings"), "{e}");
    stop.cancel();
}
