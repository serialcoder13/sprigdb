//! An in-process fake Kubernetes API server (axum on 127.0.0.1:0) for the
//! operator's tests: namespaced objects in memory, resourceVersion
//! conflicts on PUT, RFC 7386 merge patches, the status subresource,
//! label selectors, owner-reference garbage collection, and enough
//! discovery (`/api`, `/apis`, ...) for `kubectl --dry-run=client`.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::collapsible_if,
    unreachable_pub,
    missing_docs
)]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use axum::body::Bytes;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use sprig_operator::kube::merge;

#[derive(Default)]
pub struct State {
    /// collection path -> name -> object
    pub objects: HashMap<String, BTreeMap<String, Value>>,
    pub rv: u64,
    pub uid: u64,
    /// (method, path) of every request.
    pub log: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct FakeApi {
    pub addr: SocketAddr,
    pub state: Arc<Mutex<State>>,
}

fn lock(s: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    s.lock().unwrap_or_else(PoisonError::into_inner)
}

fn status(code: u16, reason: &str, message: &str) -> Response {
    let body = json!({
        "kind": "Status", "apiVersion": "v1", "status": "Failure",
        "reason": reason, "message": message, "code": code,
    });
    (
        StatusCode::from_u16(code).unwrap(),
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn ok(v: &Value) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        v.to_string(),
    )
        .into_response()
}

/// The resources discovery lists, by group/version.
fn resources(gv: &str) -> Vec<(&'static str, &'static str, bool)> {
    match gv {
        "v1" => vec![
            ("services", "Service", true),
            ("secrets", "Secret", true),
            ("events", "Event", true),
            ("namespaces", "Namespace", false),
        ],
        "sprigdb.io/v1alpha1" => vec![
            ("sprigclusters", "SprigCluster", true),
            ("sprigclusterpools", "SprigClusterPool", true),
        ],
        "k8s.mariadb.com/v1alpha1" => vec![
            ("mariadbs", "MariaDB", true),
            ("physicalbackups", "PhysicalBackup", true),
            ("pointintimerecoveries", "PointInTimeRecovery", true),
        ],
        "coordination.k8s.io/v1" => vec![("leases", "Lease", true)],
        "apiextensions.k8s.io/v1" => vec![(
            "customresourcedefinitions",
            "CustomResourceDefinition",
            false,
        )],
        _ => vec![],
    }
}

const GROUPS: &[(&str, &str)] = &[
    ("sprigdb.io", "v1alpha1"),
    ("k8s.mariadb.com", "v1alpha1"),
    ("coordination.k8s.io", "v1"),
    ("apiextensions.k8s.io", "v1"),
];

fn discovery(path: &str) -> Option<Value> {
    let list = |gv: &str| {
        json!({
            "kind": "APIResourceList", "apiVersion": "v1", "groupVersion": gv,
            "resources": resources(gv).iter().map(|(p, k, ns)| json!({
                "name": p, "singularName": k.to_ascii_lowercase(), "namespaced": ns, "kind": k,
                "verbs": ["create", "delete", "get", "list", "patch", "update", "watch"],
            })).collect::<Vec<_>>(),
        })
    };
    match path {
        "/version" => Some(json!({"major": "1", "minor": "31", "gitVersion": "v1.31.0-fake"})),
        "/api" => Some(json!({"kind": "APIVersions", "versions": ["v1"],
            "serverAddressByClientCIDRs": [{"clientCIDR": "0.0.0.0/0", "serverAddress": "127.0.0.1"}]})),
        "/api/v1" => Some(list("v1")),
        "/apis" => Some(json!({
            "kind": "APIGroupList", "apiVersion": "v1",
            "groups": GROUPS.iter().map(|(g, v)| json!({
                "name": g,
                "versions": [{"groupVersion": format!("{g}/{v}"), "version": v}],
                "preferredVersion": {"groupVersion": format!("{g}/{v}"), "version": v},
            })).collect::<Vec<_>>(),
        })),
        p => {
            let rest = p.strip_prefix("/apis/")?;
            let mut it = rest.split('/');
            let (g, v) = (it.next()?, it.next()?);
            if it.next().is_some() {
                return None;
            }
            GROUPS
                .iter()
                .any(|(gg, vv)| *gg == g && *vv == v)
                .then(|| list(&format!("{g}/{v}")))
        }
    }
}

/// (collection path, name, status subresource)
fn split(path: &str) -> Option<(String, Option<String>, bool)> {
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let base = match *segs.first()? {
        "api" => 2,
        "apis" => 3,
        _ => return None,
    };
    // .../namespaces/<ns>/<plural>[/<name>[/status]]
    if segs.get(base)? != &"namespaces" {
        return None;
    }
    let coll_end = base + 3;
    if segs.len() < coll_end {
        return None;
    }
    let coll = format!("/{}", segs[..coll_end].join("/"));
    let name = segs.get(coll_end).map(|s| (*s).to_owned());
    let sub = segs.get(coll_end + 1) == Some(&"status");
    Some((coll, name, sub))
}

fn decode_query(q: &str) -> String {
    let mut out = Vec::new();
    let b = q.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(x) = u8::from_str_radix(&q[i + 1..i + 3], 16) {
                out.push(x);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn matches(obj: &Value, selector: &str) -> bool {
    selector.split(',').filter(|s| !s.is_empty()).all(|term| {
        let (k, v) = term.split_once('=').unwrap_or((term, ""));
        obj["metadata"]["labels"][k].as_str() == Some(v)
    })
}

fn now() -> String {
    "2026-11-09T00:00:00Z".into()
}

async fn handle(
    axum::extract::State(api): axum::extract::State<Arc<Mutex<State>>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    lock(&api).log.push((method.to_string(), path.clone()));
    if method == Method::GET {
        if let Some(d) = discovery(&path) {
            return ok(&d);
        }
        if path.starts_with("/openapi") {
            return status(404, "NotFound", "no openapi");
        }
    }
    let Some((coll, name, sub)) = split(&path) else {
        return status(404, "NotFound", &format!("no route {path}"));
    };
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let mut st = lock(&api);
    match (method.as_str(), name) {
        ("GET", None) => {
            let sel = uri
                .query()
                .and_then(|q| {
                    q.split('&')
                        .find_map(|kv| kv.strip_prefix("labelSelector="))
                        .map(decode_query)
                })
                .unwrap_or_default();
            let items: Vec<Value> = st
                .objects
                .get(&coll)
                .map(|m| m.values().filter(|o| matches(o, &sel)).cloned().collect())
                .unwrap_or_default();
            ok(
                &json!({"kind": "List", "apiVersion": "v1", "metadata": {"resourceVersion": st.rv.to_string()}, "items": items}),
            )
        }
        ("GET", Some(n)) => match st.objects.get(&coll).and_then(|m| m.get(&n)) {
            Some(o) => ok(o),
            None => status(404, "NotFound", &format!("{n} not found")),
        },
        ("POST", None) => {
            let mut o = body;
            let name = match o["metadata"]["name"].as_str() {
                Some(n) if !n.is_empty() => n.to_owned(),
                _ => {
                    st.uid += 1;
                    format!(
                        "{}{:05}",
                        o["metadata"]["generateName"].as_str().unwrap_or("obj-"),
                        st.uid
                    )
                }
            };
            if st.objects.get(&coll).is_some_and(|m| m.contains_key(&name)) {
                return status(409, "AlreadyExists", &format!("{name} already exists"));
            }
            st.rv += 1;
            st.uid += 1;
            o["metadata"]["name"] = json!(name);
            o["metadata"]["uid"] = json!(format!("uid-{}", st.uid));
            o["metadata"]["resourceVersion"] = json!(st.rv.to_string());
            o["metadata"]["generation"] = json!(1);
            o["metadata"]["creationTimestamp"] = json!(now());
            if let Some(m) = o.as_object_mut() {
                m.remove("status");
            }
            st.objects.entry(coll).or_default().insert(name, o.clone());
            (
                StatusCode::CREATED,
                [("content-type", "application/json")],
                o.to_string(),
            )
                .into_response()
        }
        ("PATCH", Some(n)) => {
            let rv = st.rv + 1;
            let Some(cur) = st.objects.get_mut(&coll).and_then(|m| m.get_mut(&n)) else {
                return status(404, "NotFound", &format!("{n} not found"));
            };
            let before_spec = cur["spec"].clone();
            let before_status = cur["status"].clone();
            merge(cur, &body);
            if !sub {
                cur["status"] = before_status;
                if cur["status"].is_null() {
                    cur.as_object_mut().map(|m| m.remove("status"));
                }
            }
            if cur["spec"] != before_spec {
                let g = cur["metadata"]["generation"].as_i64().unwrap_or(1);
                cur["metadata"]["generation"] = json!(g + 1);
            }
            cur["metadata"]["resourceVersion"] = json!(rv.to_string());
            let out = cur.clone();
            st.rv = rv;
            ok(&out)
        }
        ("PUT", Some(n)) => {
            let rv = st.rv + 1;
            let Some(cur) = st.objects.get_mut(&coll).and_then(|m| m.get_mut(&n)) else {
                return status(404, "NotFound", &format!("{n} not found"));
            };
            if let Some(want) = body["metadata"]["resourceVersion"].as_str() {
                if Some(want) != cur["metadata"]["resourceVersion"].as_str() {
                    return status(
                        409,
                        "Conflict",
                        "the object has been modified; please apply your changes to the latest version",
                    );
                }
            }
            if sub {
                cur["status"] = body["status"].clone();
            } else {
                let keep_status = cur["status"].clone();
                let meta = cur["metadata"].clone();
                let spec_before = cur["spec"].clone();
                *cur = body.clone();
                cur["metadata"]["uid"] = meta["uid"].clone();
                cur["metadata"]["creationTimestamp"] = meta["creationTimestamp"].clone();
                let g = meta["generation"].as_i64().unwrap_or(1);
                cur["metadata"]["generation"] =
                    json!(if cur["spec"] == spec_before { g } else { g + 1 });
                if keep_status.is_null() {
                    cur.as_object_mut().map(|m| m.remove("status"));
                } else {
                    cur["status"] = keep_status;
                }
            }
            cur["metadata"]["resourceVersion"] = json!(rv.to_string());
            let out = cur.clone();
            st.rv = rv;
            ok(&out)
        }
        ("DELETE", Some(n)) => {
            let Some(o) = st.objects.get_mut(&coll).and_then(|m| m.remove(&n)) else {
                return status(404, "NotFound", &format!("{n} not found"));
            };
            st.rv += 1;
            // Garbage collection: everything owned by it, transitively.
            let mut gone = vec![o["metadata"]["uid"].as_str().unwrap_or_default().to_owned()];
            while let Some(uid) = gone.pop() {
                for m in st.objects.values_mut() {
                    let owned: Vec<String> = m
                        .iter()
                        .filter(|(_, v)| {
                            v["metadata"]["ownerReferences"]
                                .as_array()
                                .is_some_and(|rs| rs.iter().any(|r| r["uid"] == uid.as_str()))
                        })
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in owned {
                        if let Some(v) = m.remove(&k) {
                            gone.push(v["metadata"]["uid"].as_str().unwrap_or_default().to_owned());
                        }
                    }
                }
            }
            ok(&json!({"kind": "Status", "status": "Success"}))
        }
        _ => status(405, "MethodNotAllowed", "not supported by the fake"),
    }
}

impl FakeApi {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let app = axum::Router::new()
            .fallback(handle)
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, state }
    }

    pub fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn get(&self, coll: &str, name: &str) -> Option<Value> {
        lock(&self.state)
            .objects
            .get(coll)
            .and_then(|m| m.get(name))
            .cloned()
    }

    pub fn names(&self, coll: &str) -> Vec<String> {
        lock(&self.state)
            .objects
            .get(coll)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Changes an object as another controller would (merge, new RV).
    pub fn mutate(&self, coll: &str, name: &str, patch: &Value) {
        let mut st = lock(&self.state);
        st.rv += 1;
        let rv = st.rv;
        let o = st
            .objects
            .get_mut(coll)
            .and_then(|m| m.get_mut(name))
            .unwrap_or_else(|| panic!("{coll}/{name} missing"));
        merge(o, patch);
        o["metadata"]["resourceVersion"] = json!(rv.to_string());
    }

    /// Puts an object as is (fixtures: Secrets, CRs).
    pub fn put(&self, coll: &str, obj: Value) {
        let mut st = lock(&self.state);
        st.rv += 1;
        st.uid += 1;
        let mut o = obj;
        o["metadata"]["resourceVersion"] = json!(st.rv.to_string());
        if o["metadata"]["uid"].is_null() {
            o["metadata"]["uid"] = json!(format!("uid-{}", st.uid));
        }
        if o["metadata"]["generation"].is_null() {
            o["metadata"]["generation"] = json!(1);
        }
        let name = o["metadata"]["name"].as_str().unwrap().to_owned();
        st.objects
            .entry(coll.to_owned())
            .or_default()
            .insert(name, o);
    }

    pub fn count(&self, method: &str, prefix: &str) -> usize {
        lock(&self.state)
            .log
            .iter()
            .filter(|(m, p)| m == method && p.starts_with(prefix))
            .count()
    }

    pub fn clear_log(&self) {
        lock(&self.state).log.clear();
    }
}
