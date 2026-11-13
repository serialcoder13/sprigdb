//! Step 5 of the reconcile: register the cluster through the DC
//! controller (chapter 27, "The Kubernetes operator"), with
//! `provisioner = kubernetes`, so probes start.
//!
//! [`AdminRegistrar`] calls the controller's Admin API,
//! `POST /admin/v1/clusters` (chapter 09's `sprigctl cluster register`
//! call), with the nodes' advertised addresses and the credential
//! references the operator wrote.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value, json};
use sprig_cluster::record::{NodeSpec, SecretRefs, TopologyKind};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

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

    /// Replaces the record's nodes after a scale or a replacement
    /// (`:setNodes`, r2opfix). The default refuses (registrars that can't).
    ///
    /// # Errors
    /// The controller's refusal or a transport error.
    async fn set_nodes(&self, cluster_id: &str, nodes: &[NodeSpec]) -> Result<()> {
        let _ = (cluster_id, nodes);
        Err(SprigError::new(
            SprigCode::ProvisionerUnsupported,
            "this registrar can't update a record's nodes",
        ))
    }

    /// Retires a drained cluster's record (`:retire`, r2opfix).
    ///
    /// # Errors
    /// 39106 unless the cluster is drained; a transport error.
    async fn retire(&self, cluster_id: &str) -> Result<()> {
        let _ = cluster_id;
        Err(SprigError::new(
            SprigCode::ProvisionerUnsupported,
            "this registrar can't retire a record",
        ))
    }
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
        // The record's `declared.failover` is `provider` or `none`: on
        // Kubernetes the provider side fails over, whether mariadb-operator
        // alone (`operator`) or with the SprigDB operator choosing the
        // replica (`sprig`; r2opfix: `sprig` was refused before).
        "declared": {
            "failover": match r.failover_driver {
                FailoverDriver::Operator | FailoverDriver::Sprig => "provider",
            },
        },
    })
}

/// [`ClusterRegistrar`] over the controller's Admin API (`http://`, or
/// `https://` checked against a CA bundle).
#[derive(Clone)]
pub struct AdminRegistrar {
    host: String,
    port: u16,
    token: Option<String>,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl std::fmt::Debug for AdminRegistrar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminRegistrar")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("https", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

impl AdminRegistrar {
    /// The controller at `endpoint` (`http://host:port`), with the Admin
    /// API's bearer token.
    ///
    /// # Errors
    /// Internal for an endpoint that isn't `http://host:port`.
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        Self::with_ca(endpoint, token, None)
    }

    /// The controller at `endpoint`: `http://host:port`, or
    /// `https://host:port` whose certificate is checked against `ca_pem`
    /// (PEM, one or more CA certificates) and must name `host`.
    ///
    /// # Errors
    /// `ConfigInvalid` for an endpoint that isn't `http(s)://host:port`, an
    /// `https://` endpoint without a CA bundle, or a CA bundle with no
    /// usable certificate.
    pub fn with_ca(endpoint: &str, token: Option<String>, ca_pem: Option<&[u8]>) -> Result<Self> {
        let bad = |why: &str| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("controller endpoint {endpoint}: {why}"),
            )
        };
        let (https, rest) = if let Some(r) = endpoint.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = endpoint.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(bad("expected http://host:port or https://host:port"));
        };
        let rest = rest.trim_end_matches('/');
        let (host, port) = rest
            .rsplit_once(':')
            .and_then(|(h, p)| Some((h.to_owned(), p.parse().ok()?)))
            .ok_or_else(|| bad("no port"))?;
        let tls = if https {
            let pem = ca_pem.ok_or_else(|| {
                bad("https needs the CA bundle its certificate is checked against")
            })?;
            let mut roots = rustls::RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(pem) {
                let c = c.map_err(|e| bad(&format!("CA bundle: {e}")))?;
                roots.add(c).map_err(|e| bad(&format!("CA bundle: {e}")))?;
            }
            if roots.is_empty() {
                return Err(bad("the CA bundle holds no certificate"));
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut cc = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| bad(&e.to_string()))?
                .with_root_certificates(roots)
                .with_no_client_auth();
            cc.alpn_protocols = vec![b"http/1.1".to_vec()];
            let name = ServerName::try_from(host.clone()).map_err(|e| bad(&e.to_string()))?;
            Some((TlsConnector::from(Arc::new(cc)), name))
        } else {
            None
        };
        Ok(Self {
            host,
            port,
            token,
            tls,
        })
    }

    /// Whether calls go over HTTPS.
    #[must_use]
    pub fn is_https(&self) -> bool {
        self.tls.is_some()
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
        let mut send = if let Some((tls, name)) = &self.tls {
            let s = tls.connect(name.clone(), tcp).await.map_err(|e| t(&e))?;
            let (send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(s))
                    .await
                    .map_err(|e| t(&e))?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            send
        } else {
            let (send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
                    .await
                    .map_err(|e| t(&e))?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            send
        };
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

    async fn set_nodes(&self, cluster_id: &str, nodes: &[NodeSpec]) -> Result<()> {
        let (status, v) = self
            .post(
                &format!("/admin/v1/clusters/{cluster_id}:setNodes"),
                &json!({"nodes": nodes}),
            )
            .await?;
        api_result(status, &v)
    }

    async fn retire(&self, cluster_id: &str) -> Result<()> {
        let (status, v) = self
            .post(
                &format!("/admin/v1/clusters/{cluster_id}:retire"),
                &json!({}),
            )
            .await?;
        api_result(status, &v)
    }
}

/// `Ok` for a 2xx answer; the API's error otherwise.
fn api_result(status: u16, v: &Value) -> Result<()> {
    if (200..300).contains(&status) {
        return Ok(());
    }
    let msg = v["message"].as_str().unwrap_or("refused").to_owned();
    let code = v["code"]
        .as_u64()
        .and_then(|c| u16::try_from(c).ok())
        .and_then(|n| SprigCode::ALL.iter().copied().find(|c| c.number() == n))
        .unwrap_or(SprigCode::Internal);
    Err(SprigError::new(code, msg))
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

    #[test]
    fn admin_endpoints_parse_and_https_needs_a_ca() {
        let r = AdminRegistrar::new("http://ctl.ns.svc:7480/", None).unwrap();
        assert_eq!(
            (r.host.as_str(), r.port, r.is_https()),
            ("ctl.ns.svc", 7480, false)
        );
        let e = AdminRegistrar::new("https://ctl.ns.svc:7480", None).unwrap_err();
        assert!(e.to_string().contains("CA bundle"), "{e}");
        let e = AdminRegistrar::with_ca("https://ctl.ns.svc:7480", None, Some(b"")).unwrap_err();
        assert!(e.to_string().contains("no certificate"), "{e}");
        assert!(AdminRegistrar::new("ctl:7480", None).is_err());
        assert!(AdminRegistrar::new("http://ctl", None).is_err());
        // A throwaway self-signed CA (P-256), only parsed here.
        let ca = "-----BEGIN CERTIFICATE-----\n\
                  MIIBezCCASGgAwIBAgIUQ5nxo3ua30mko+2hmSl2gsNB184wCgYIKoZIzj0EAwIw\n\
                  EjEQMA4GA1UEAwwHdGVzdC1jYTAgFw0yNjExMTIyMDU1MDdaGA8yMTI2MTAxOTIw\n\
                  NTUwN1owEjEQMA4GA1UEAwwHdGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEH\n\
                  A0IABIir9VkfvBeCeRX8UiLNZLAawt0VdRS7QExf9uN4g9w40Fugvdwp0HIpXI8h\n\
                  tKYer3BfmmlnFPIxonoN5actKVmjUzBRMB0GA1UdDgQWBBRZ9uNYwUsDNIviMCnm\n\
                  U2k6Ge2PKzAfBgNVHSMEGDAWgBRZ9uNYwUsDNIviMCnmU2k6Ge2PKzAPBgNVHRMB\n\
                  Af8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIQC5rB0p8Rf8U3l6vTQ7DiK0tjPa\n\
                  p7DAGVGMkNx/o3ZQbAIgQCuHa9iN/SvhbUqSwssAYFGaAoL7YoF3WT4xfXUTht8=\n\
                  -----END CERTIFICATE-----";
        let r =
            AdminRegistrar::with_ca("https://ctl.ns.svc:7480", None, Some(ca.as_bytes())).unwrap();
        assert!(r.is_https());
    }
}
