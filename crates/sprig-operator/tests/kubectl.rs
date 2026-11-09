//! `kubectl apply --dry-run=client` reads the generated manifests, against
//! the fake API server's discovery (no cluster; nothing is downloaded).
//! Skipped when `kubectl` isn't on the path.

#![allow(clippy::unwrap_used)]

mod support;

use std::process::Command;

use serde_json::json;
use sprig_operator::crd::{self, SprigCluster};
use sprig_operator::{OperatorConfig, mapping, yaml};

fn have_kubectl() -> bool {
    Command::new("kubectl")
        .args(["version", "--client"])
        .output()
        .is_ok_and(|o| o.status.success())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kubectl_reads_the_crds_and_the_generated_resources() {
    if !have_kubectl() {
        eprintln!("skipped: no kubectl on the path");
        return;
    }
    let api = support::FakeApi::start().await;
    let dir = std::env::temp_dir().join(format!("sprig-op-kubectl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let kubeconfig = dir.join("kubeconfig");
    std::fs::write(
        &kubeconfig,
        format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: fake\n  cluster:\n    server: {}\n\
             contexts:\n- name: fake\n  context:\n    cluster: fake\n    namespace: sprigdb\n    user: fake\n\
             current-context: fake\nusers:\n- name: fake\n  user:\n    token: fake\n",
            api.base()
        ),
    )
    .unwrap();

    // The CRDs, as the chart ships them.
    let crds = dir.join("crds.yaml");
    std::fs::write(&crds, yaml::to_yaml_stream(&crd::crds())).unwrap();
    // A SprigCluster and everything the operator makes from it.
    let sc: SprigCluster = serde_json::from_value(json!({
        "apiVersion": "sprigdb.io/v1alpha1", "kind": "SprigCluster",
        "metadata": {"name": "fra1-c7", "namespace": "sprigdb", "uid": "u-1"},
        "spec": {"topology": "replication", "nodes": 3, "mariadb": {"image": "mariadb:12.3"},
                 "storage": {"size": "500Gi"}, "backup": {"schedule": "0 2 * * *", "target": "s3://b"}},
    }))
    .unwrap();
    let mut cfg = OperatorConfig::new("sprigdb");
    cfg.agent_image = Some("ghcr.io/sprigdb/sprigdb:0.9.0".into());
    let mut docs = vec![
        serde_json::to_value(&sc).unwrap(),
        mapping::mariadb(&sc, &cfg),
    ];
    docs.extend(mapping::backups(&sc).into_iter().map(|(_, v)| v));
    docs.extend(mapping::node_services(&sc, &cfg));
    if let Some(o) = docs[0].as_object_mut() {
        o.remove("status");
    }
    let resources = dir.join("resources.yaml");
    std::fs::write(&resources, yaml::to_yaml_stream(&docs)).unwrap();

    for (file, expect) in [(&crds, 2usize), (&resources, docs.len())] {
        let out = tokio::task::spawn_blocking({
            let (kubeconfig, file, cache) = (kubeconfig.clone(), file.clone(), dir.join("cache"));
            move || {
                Command::new("kubectl")
                    .arg("--kubeconfig")
                    .arg(&kubeconfig)
                    .arg("--cache-dir")
                    .arg(&cache)
                    .args([
                        "apply",
                        "--dry-run=client",
                        "--validate=false",
                        "-o",
                        "name",
                        "-f",
                    ])
                    .arg(&file)
                    .output()
                    .unwrap()
            }
        })
        .await
        .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "kubectl failed on {}:\n{stdout}\n{stderr}",
            file.display()
        );
        assert_eq!(stdout.lines().count(), expect, "{stdout}");
        eprintln!("kubectl --dry-run=client:\n{stdout}");
    }
    // Client dry-run never writes.
    assert_eq!(api.count("POST", "/"), 0);
    assert_eq!(api.count("PATCH", "/"), 0);
    let _ = std::fs::remove_dir_all(&dir);
}
