//! Step 5 of the reconcile: register the cluster through the DC
//! controller (chapter 27, "The Kubernetes operator"), with
//! `provisioner = kubernetes`, so probes start.
//!
//! [`AdminRegistrar`] calls the controller's Admin API,
//! `POST /admin/v1/clusters` (chapter 09's `sprigctl cluster register`
//! call), with the nodes' advertised addresses and the credential
//! references the operator wrote.

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use serde_json::{Value, json};
use sprig_cluster::record::{NodeSpec, SecretRefs, TopologyKind};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::net::TcpStream;

use crate::crd::FailoverDriver;

/// What the operator registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRegistration {
    /// The `SprigCluster`'s name: the record's name.
    pub name: String,
    /// The topology.
    pub topology: TopologyKind,
    /// The nodes, `n0`, `n1`, ... at their advertised addresses.
    pub nodes: Vec<NodeSpec>,
    /// Credential references.
    pub secrets: SecretRefs,
    /// Who fails the primary over.
    pub failover_driver: FailoverDriver,
}

/// A registered cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// The record's id.
    pub cluster_id: String,
    /// Its number (`gtid_domain_id`).
    pub cluster_number: u32,
}

/// Registers clusters with the DC controller.
#[async_trait]
pub trait ClusterRegistrar: Send + Sync + 'static {
    /// Registers `r`; registering the same name again returns the existing
    /// record.
    ///
    /// # Errors
    /// The controller's refusal (39511 for failed prechecks, 39510 when it
    /// doesn't take Kubernetes clusters), or a transport error.
    async fn register(&self, r: &ClusterRegistration) -> Result<Registered>;
}

/// The request body of `POST /admin/v1/clusters`.
#[must_use]
pub fn request_body(r: &ClusterRegistration) -> Value {
    json!({
        "name": r.name,
        "topology": r.topology,
        "provisioner": "kubernetes",
        "nodes": r.nodes,
        "secrets": r.secrets,
        "declared": {
            "failover": match r.failover_driver {
                FailoverDriver::Operator => "provider",
                FailoverDriver::Sprig => "sprig",
            },
        },
    })
}

/// [`ClusterRegistrar`] over the controller's Admin API (`http://`).
#[derive(Debug, Clone)]
pub struct AdminRegistrar {
    host: String,
    port: u16,
    token: Option<String>,
}

impl AdminRegistrar {
    /// The controller at `endpoint` (`http://host:port`), with the Admin
    /// API's bearer token.
    ///
    /// # Errors
    /// Internal for an endpoint that isn't `http://host:port`.
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        let rest = endpoint
            .strip_prefix("http://")
            .ok_or_else(|| {
                SprigError::internal(format!(
                    "controller endpoint {endpoint}: expected http://host:port"
                ))
            })?
            .trim_end_matches('/');
        let (host, port) = rest
            .rsplit_once(':')
            .and_then(|(h, p)| Some((h.to_owned(), p.parse().ok()?)))
            .ok_or_else(|| {
                SprigError::internal(format!("controller endpoint {endpoint}: no port"))
            })?;
        Ok(Self { host, port, token })
    }

    async fn post(&self, path: &str, body: &Value) -> Result<(u16, Value)> {
        let t = |e: &dyn std::fmt::Display| {
            SprigError::new(
                SprigCode::MetaUnavailable,
                format!("controller admin API: {e}"),
            )
        };
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| t(&e))?;
        let (mut send, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
                .await
                .map_err(|e| t(&e))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut b = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(hyper::header::HOST, format!("{}:{}", self.host, self.port))
            .header(hyper::header::CONTENT_TYPE, "application/json");
        if let Some(tok) = &self.token {
            b = b.header(hyper::header::AUTHORIZATION, format!("Bearer {tok}"));
        }
        let req = b
            .body(Full::new(Bytes::from(body.to_string())))
            .map_err(|e| t(&e))?;
        let resp = send.send_request(req).await.map_err(|e| t(&e))?;
        let status = resp.status().as_u16();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| t(&e))?
            .to_bytes();
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }
}

/// Reads the registered record out of the Admin API's answer.
///
/// # Errors
/// The API's error (its `code` and `message`), or internal for an answer
/// without a cluster.
pub fn parse_response(status: u16, v: &Value) -> Result<Registered> {
    if (200..300).contains(&status) {
        let c = &v["cluster"];
        return Ok(Registered {
            cluster_id: c["cluster_id"]
                .as_str()
                .ok_or_else(|| SprigError::internal("registration answer without cluster_id"))?
                .to_owned(),
            cluster_number: c["cluster_number"]
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(0),
        });
    }
    let msg = v["message"]
        .as_str()
        .or_else(|| v["error"]["message"].as_str())
        .unwrap_or("registration refused")
        .to_owned();
    let code = v["code"]
        .as_u64()
        .or_else(|| v["error"]["code"].as_u64())
        .and_then(|c| u16::try_from(c).ok())
        .and_then(|n| SprigCode::ALL.iter().copied().find(|c| c.number() == n))
        .unwrap_or(SprigCode::Internal);
    Err(SprigError::new(code, msg))
}

#[async_trait]
impl ClusterRegistrar for AdminRegistrar {
    async fn register(&self, r: &ClusterRegistration) -> Result<Registered> {
        let (status, v) = self.post("/admin/v1/clusters", &request_body(r)).await?;
        parse_response(status, &v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::NodeId;

    #[test]
    fn body_and_answers() {
        let r = ClusterRegistration {
            name: "fra1-c7".into(),
            topology: TopologyKind::PrimaryReplica,
            nodes: vec![NodeSpec {
                node: NodeId::new("n0").unwrap_or_else(|_| unreachable!()),
                addr: "10.0.0.5:3306".into(),
                zone: None,
            }],
            secrets: SecretRefs {
                ctl: "file:/etc/sprigdb/creds/fra1-c7.toml#sprig_ctl".into(),
                repl: "file:/etc/sprigdb/creds/fra1-c7.toml#sprig_repl".into(),
            },
            failover_driver: FailoverDriver::Operator,
        };
        let b = request_body(&r);
        assert_eq!(b["provisioner"], "kubernetes");
        assert_eq!(b["topology"], "primary_replica");
        assert_eq!(b["nodes"][0]["addr"], "10.0.0.5:3306");
        let ok = parse_response(
            200,
            &json!({"cluster": {"cluster_id": "fra1-c3", "cluster_number": 3}}),
        );
        assert_eq!(
            ok.ok(),
            Some(Registered {
                cluster_id: "fra1-c3".into(),
                cluster_number: 3
            })
        );
        let e = parse_response(400, &json!({"code": 39510, "message": "no"})).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ProvisionerUnsupported));
    }
}
