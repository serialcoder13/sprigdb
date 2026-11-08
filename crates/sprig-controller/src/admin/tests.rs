//! The Admin API over `MemStore`: leader checks, refusals with their
//! codes, the cluster calls on records whose nodes refuse connections, and
//! the HTTP face (routes, error bodies, bearer tokens).

#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::sync::Arc;

use serde_json::{Value, json};
use sprig_cluster::record::{AdminState, TopologyKind};
use sprig_common::SprigCode;
use sprig_common::time::SystemClock;
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{Op, Record, Txn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use super::*;
use crate::monitor::tests::{fixture, lead, unreachable};
use crate::runtime::Status;

struct Admin {
    svc: Arc<AdminService>,
    status: watch::Sender<Status>,
    f: crate::monitor::tests::Fixture,
}

async fn admin(dc: &str, registered: bool) -> Admin {
    let f = fixture(dc).await;
    if registered {
        let rec = Record::new(DcRecord {
            dc_id: DcId::new(dc).unwrap(),
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: serde_json::Map::new(),
            global_voter: true,
        });
        f.global
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key: Tier::Global.key(&format!("dcs/{dc}")).unwrap(),
                    value: rec.encode().unwrap(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .unwrap();
    }
    let (status, rx) = watch::channel(Status::default());
    let stores = Stores {
        dc: Arc::new(f.dc.clone()),
        global: Arc::new(f.global.clone()),
    };
    let svc = Arc::new(AdminService::new(
        stores,
        Arc::clone(&f.monitor),
        rx,
        Arc::new(SystemClock),
    ));
    Admin { svc, status, f }
}

/// Makes the service the DC leader.
async fn make_leader(a: &Admin) {
    let guard = lead(&a.f.dc).await;
    let sprig_meta::Cmp::CreateRev(_, token) = guard[0].clone() else {
        panic!("a create-revision guard");
    };
    a.status.send_modify(|s| s.dc_leader = Some(token));
}

fn request(name: &str) -> RegisterClusterRequest {
    let r = unreachable(name, TopologyKind::SingleNode);
    RegisterClusterRequest {
        dc: None,
        name: r.name,
        topology: r.topology,
        provisioner: ProvisionerKind::External,
        nodes: r.nodes,
        secrets: r.secrets,
        capacity: r.capacity,
        declared: None,
        dry_run: false,
    }
}

fn code(e: &SprigError) -> Option<SprigCode> {
    e.code()
}

#[tokio::test]
async fn registration_is_refused_with_the_right_codes() {
    let a = admin("adm-a", false).await;
    let e = a.svc.register_cluster(request("c")).await.unwrap_err();
    assert_eq!(code(&e), Some(SprigCode::UnknownDc));
    let a = admin("adm-b", true).await;
    let mut other = request("c");
    other.dc = Some(DcId::new("elsewhere").unwrap());
    assert_eq!(
        code(&a.svc.register_cluster(other).await.unwrap_err()),
        Some(SprigCode::AdminRequestInvalid)
    );
    let mut k8s = request("c");
    k8s.provisioner = ProvisionerKind::Kubernetes;
    assert_eq!(
        code(&a.svc.register_cluster(k8s).await.unwrap_err()),
        Some(SprigCode::ProvisionerUnsupported)
    );
    let e = a.svc.register_cluster(request("c")).await.unwrap_err();
    assert_eq!(code(&e), Some(SprigCode::NotLeader));
    assert_eq!(e.message, "dc adm-b has no leader yet; try again");

    make_leader(&a).await;
    let mut dry = request("c");
    dry.dry_run = true;
    let r = a.svc.register_cluster(dry).await.unwrap();
    assert!(r.cluster.is_none());
    assert_eq!(
        r.precheck.blocking,
        [
            "node n1: sprig_ctl credential: secret env:SPRIG_TEST_MONITOR_CTL: the variable isn't set"
        ]
    );
    let e = a.svc.register_cluster(request("c")).await.unwrap_err();
    assert_eq!(code(&e), Some(SprigCode::ClusterPrecheckFailed));
    assert!(
        e.message
            .starts_with("cluster \"c\" failed 1 registration precheck(s):\n- node n1")
    );
    assert!(a.svc.list_clusters().await.unwrap().clusters.is_empty());

    // A name in use is refused before any probing.
    a.f.monitor
        .registry()
        .register(unreachable("taken", TopologyKind::SingleNode))
        .await
        .unwrap();
    assert_eq!(
        code(&a.svc.register_cluster(request("taken")).await.unwrap_err()),
        Some(SprigCode::ClusterExists)
    );
}

#[tokio::test]
async fn cluster_calls_on_a_registered_record() {
    let a = admin("adm-c", true).await;
    let reg = a.f.monitor.registry().clone();
    let s = reg
        .register(unreachable("u1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.to_string();
    reg.set_admin_state(&s.body().cluster_id, AdminState::Active, vec![])
        .await
        .unwrap();

    let list = a.svc.list_clusters().await.unwrap();
    assert_eq!(list.dc.as_str(), "adm-c");
    assert_eq!(list.clusters[0].record.name, "u1");
    let v = a.svc.get_cluster(&id).await.unwrap();
    assert_eq!(v.record.admin_state, AdminState::Active);
    assert!(v.health_report.is_none());
    assert_eq!(
        code(&a.svc.get_cluster("adm-c-c9").await.unwrap_err()),
        Some(SprigCode::ClusterNotFound)
    );
    assert_eq!(
        code(&a.svc.get_cluster("NOT AN ID").await.unwrap_err()),
        Some(SprigCode::AdminRequestInvalid)
    );
    assert_eq!(
        code(
            &a.svc
                .probe_cluster(&id, ProbeClusterRequest::default())
                .await
                .unwrap_err()
        ),
        Some(SprigCode::NotLeader)
    );

    make_leader(&a).await;
    let p = a
        .svc
        .probe_cluster(&id, ProbeClusterRequest { full: true })
        .await
        .unwrap();
    assert_eq!(p.depth, ProbeDepth::Deep);
    assert!(p.cluster.health_report.is_some());
    assert_eq!(
        p.cluster.record.health.as_ref().unwrap().state,
        sprig_cluster::health::ClusterHealth::Unknown
    );
    let d = a.svc.drain_cluster(&id).await.unwrap();
    assert_eq!(d.record.admin_state, AdminState::Draining);
    assert_eq!(
        a.svc.drain_cluster(&id).await.unwrap().record.admin_state,
        AdminState::Draining
    );
    let probing = reg
        .register(unreachable("u2", TopologyKind::SingleNode))
        .await
        .unwrap();
    let e = a
        .svc
        .drain_cluster(probing.body().cluster_id.as_str())
        .await
        .unwrap_err();
    assert_eq!(code(&e), Some(SprigCode::ClusterStateInvalid));
    assert_eq!(
        e.message,
        "cluster adm-c-c2 is probing; only an active cluster can drain"
    );
}

#[tokio::test]
async fn dc_calls() {
    let a = admin("adm-d", true).await;
    let r = a
        .svc
        .register_dc(RegisterDcRequest {
            dc: DcId::new("ams1").unwrap(),
            kind: "full".into(),
            tags: serde_json::from_value(json!({"jurisdiction": "EU"})).unwrap(),
            global_voter: false,
        })
        .await
        .unwrap();
    assert!(r.created);
    assert_eq!(r.record.state, "joining");
    let again = a
        .svc
        .register_dc(RegisterDcRequest {
            dc: DcId::new("ams1").unwrap(),
            kind: "full".into(),
            tags: serde_json::Map::new(),
            global_voter: false,
        })
        .await
        .unwrap();
    assert!(!again.created);
    assert_eq!(a.svc.list_dcs().await.unwrap().dcs.len(), 2);
    let me = a.svc.get_dc("adm-d").await.unwrap();
    assert!(me.leader.is_none());
    make_leader(&a).await;
    assert_eq!(
        code(&a.svc.get_dc("fra1").await.unwrap_err()),
        Some(SprigCode::UnknownDc)
    );
    let bad = a
        .svc
        .register_dc(RegisterDcRequest {
            dc: DcId::new("x1").unwrap(),
            kind: "edge".into(),
            tags: serde_json::Map::new(),
            global_voter: false,
        })
        .await
        .unwrap_err();
    assert_eq!(code(&bad), Some(SprigCode::AdminRequestInvalid));
}

/// One HTTP/1.1 exchange with `Connection: close`; the status and the
/// parsed JSON body.
async fn call(addr: std::net::SocketAddr, req: &str) -> (u16, Value) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let status: u16 = out[9..12].parse().unwrap();
    let body = out.split_once("\r\n\r\n").unwrap().1;
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn get(path: &str, token: Option<&str>) -> String {
    let auth = token.map_or(String::new(), |t| format!("authorization: Bearer {t}\r\n"));
    format!("GET {path} HTTP/1.1\r\nhost: x\r\n{auth}connection: close\r\n\r\n")
}

fn post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn the_rest_face() {
    let a = admin("adm-e", true).await;
    let stop = sprig_common::CancellationToken::new();
    let l = http::bind("127.0.0.1:0".parse().unwrap(), None)
        .await
        .unwrap();
    let addr = l.local_addr().unwrap();
    let server = tokio::spawn(http::serve(
        l,
        http::router(Arc::clone(&a.svc), None),
        stop.clone(),
    ));
    let (s, v) = call(addr, &get("/admin/v1/clusters", None)).await;
    assert_eq!((s, v["dc"].as_str()), (200, Some("adm-e")));
    let (s, v) = call(addr, &get("/admin/v1/clusters/adm-e-c4", None)).await;
    assert_eq!((s, v["code"].as_u64()), (404, Some(39104)));
    assert_eq!(v["name"], "ER_SPRIG_CLUSTER_NOT_FOUND");
    assert_eq!(v["trace_id"].as_str().unwrap().len(), 32);
    let (s, v) = call(addr, &post("/admin/v1/clusters/adm-e-c4:probe", "")).await;
    assert_eq!((s, v["code"].as_u64()), (503, Some(39504)));
    let (s, v) = call(addr, &post("/admin/v1/clusters/adm-e-c4:explode", "")).await;
    assert_eq!((s, v["code"].as_u64()), (400, Some(39792)));
    let (s, v) = call(addr, &post("/admin/v1/clusters", "{\"name\": 1}")).await;
    assert_eq!((s, v["code"].as_u64()), (400, Some(39792)));
    let (s, v) = call(addr, &get("/admin/v1/nothing", None)).await;
    assert_eq!((s, v["code"].as_u64()), (404, Some(39792)));
    let body = r#"{"dc":"lon1","tags":{"country":"GB"}}"#;
    let (s, v) = call(addr, &post("/admin/v1/dcs", body)).await;
    assert_eq!((s, v["created"].as_bool()), (200, Some(true)));
    let (s, v) = call(addr, &get("/admin/v1/dcs/lon1", None)).await;
    assert_eq!((s, v["state"].as_str()), (200, Some("joining")));
    stop.cancel();
    server.await.unwrap().unwrap();

    // With a token.
    let stop = sprig_common::CancellationToken::new();
    let l = http::bind("127.0.0.1:0".parse().unwrap(), Some("t0k"))
        .await
        .unwrap();
    let addr = l.local_addr().unwrap();
    let server = tokio::spawn(http::serve(
        l,
        http::router(Arc::clone(&a.svc), Some("t0k".into())),
        stop.clone(),
    ));
    let (s, v) = call(addr, &get("/admin/v1/dcs", None)).await;
    assert_eq!((s, v["code"].as_u64()), (401, Some(39793)));
    let (s, _) = call(addr, &get("/admin/v1/dcs", Some("t0kx"))).await;
    assert_eq!(s, 401);
    let (s, v) = call(addr, &get("/admin/v1/dcs", Some("t0k"))).await;
    assert_eq!((s, v["dcs"].as_array().map(Vec::len)), (200, Some(2)));
    stop.cancel();
    server.await.unwrap().unwrap();

    // A public address needs a token.
    let e = http::bind("0.0.0.0:0".parse().unwrap(), None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
}
