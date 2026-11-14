//! Step 5 of the reconcile: register the cluster through the DC
//! controller, with `provisioner = kubernetes`, so probes start.
//!
//! [`AdminRegistrar`] calls the controller's Admin API,
//! `POST /admin/v1/clusters` (the call `sprigctl cluster register`
//! makes), with the nodes' advertised addresses and the credential
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
    /// (`:setNodes`). The default refuses (registrars that can't).
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

    /// Retires a drained cluster's record (`:retire`).
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
        // replica (`sprig`, which the controller used to refuse).
        "declared": {
            "failover": match r.failover_driver {
                FailoverDriver::Operator | FailoverDriver::Sprig => "provider",
            },
        },
    })
}

/// [`ClusterRegistrar`] over the controller's Admin API (`http://`, or
/// `https://` checked against a CA bundle).
///
/// Registration, `:setNodes` and `:retire` are served only by the DC
/// leader; another controller answers 39504 naming it. With a leader
/// address template ([`Self::with_leader_template`]), such a refusal is
/// sent again to the leader's own address, and later calls go there first
/// until it fails or stops leading. Without one, the refusal is returned and
/// the reconcile retries on its next pass.
#[derive(Clone)]
pub struct AdminRegistrar {
    host: String,
    port: u16,
    token: Option<String>,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    /// `http(s)://{leader}...:port`: where the controller named `{leader}`
    /// serves the Admin API.
    leader_template: Option<String>,
    /// The leader last reached through the template.
    leader: Arc<std::sync::Mutex<Option<String>>>,
}

impl std::fmt::Debug for AdminRegistrar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminRegistrar")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("https", &self.tls.is_some())
            .field("leader_template", &self.leader_template)
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
            leader_template: None,
            leader: Arc::default(),
        })
    }

    /// Follows the DC leader: a 39504 refusal naming controller `X` is sent
    /// again to `template` with `{leader}` replaced by `X` (the chart's
    /// `https://{leader}.sprigdb-controller-pods.<ns>.svc:7480`, the
    /// address each controller advertises). The scheme must match the
    /// endpoint's, so the same CA bundle checks it.
    ///
    /// # Errors
    /// `ConfigInvalid` for a template without `{leader}`, or one that isn't
    /// `http(s)://host:port` with the endpoint's scheme.
    pub fn with_leader_template(mut self, template: Option<String>) -> Result<Self> {
        if let Some(t) = &template {
            let bad = |why: &str| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("controller leader address {t}: {why}"),
                )
            };
            if !t.contains("{leader}") {
                return Err(bad("expected {leader} where the controller's name goes"));
            }
            let scheme = if self.is_https() {
                "https://"
            } else {
                "http://"
            };
            if !t.starts_with(scheme) {
                return Err(bad(&format!("expected {scheme}, as the endpoint")));
            }
            split_endpoint(&t.replace("{leader}", "x")).ok_or_else(|| bad("no port"))?;
        }
        self.leader_template = template;
        Ok(self)
    }

    /// Whether calls go over HTTPS.
    #[must_use]
    pub fn is_https(&self) -> bool {
        self.tls.is_some()
    }

    /// The Admin address of the controller named `leader`, from the
    /// template.
    fn leader_addr(&self, leader: &str) -> Option<(String, u16)> {
        split_endpoint(&self.leader_template.as_ref()?.replace("{leader}", leader))
    }

    /// Sends `path` to the leader: the one last reached, else the endpoint,
    /// following its 39504 refusal to the controller it names.
    async fn call(&self, path: &str, body: &Value) -> Result<(u16, Value)> {
        let known = self.leader.lock().ok().and_then(|l| l.clone());
        if let Some((host, port)) = known.as_deref().and_then(|l| self.leader_addr(l)) {
            match self.post_to(&host, port, path, body).await {
                Ok(r) if not_leader(&r).is_none() => return Ok(r),
                // Gone or no longer leading: back to the endpoint.
                _ => self.remember(None),
            }
        }
        let r = self.post_to(&self.host, self.port, path, body).await?;
        let Some(leader) = not_leader(&r) else {
            return Ok(r);
        };
        let Some((host, port)) = self.leader_addr(&leader) else {
            return Ok(r);
        };
        tracing::debug!(%leader, "controller admin API: following the DC leader");
        let r = self.post_to(&host, port, path, body).await?;
        if not_leader(&r).is_none() {
            self.remember(Some(leader));
        }
        Ok(r)
    }

    fn remember(&self, leader: Option<String>) {
        if let Ok(mut l) = self.leader.lock() {
            *l = leader;
        }
    }

    async fn post_to(
        &self,
        host: &str,
        port: u16,
        path: &str,
        body: &Value,
    ) -> Result<(u16, Value)> {
        let t = |e: &dyn std::fmt::Display| {
            SprigError::new(
                SprigCode::MetaUnavailable,
                format!("controller admin API: {e}"),
            )
        };
        let tcp = TcpStream::connect((host, port)).await.map_err(|e| t(&e))?;
        let mut send = if let Some((tls, name)) = &self.tls {
            let name = if host == self.host {
                name.clone()
            } else {
                ServerName::try_from(host.to_owned()).map_err(|e| t(&e))?
            };
            let s = tls.connect(name, tcp).await.map_err(|e| t(&e))?;
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
            .header(hyper::header::HOST, format!("{host}:{port}"))
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
        let (status, v) = self.call("/admin/v1/clusters", &request_body(r)).await?;
        parse_response(status, &v)
    }

    async fn set_nodes(&self, cluster_id: &str, nodes: &[NodeSpec]) -> Result<()> {
        let (status, v) = self
            .call(
                &format!("/admin/v1/clusters/{cluster_id}:setNodes"),
                &json!({"nodes": nodes}),
            )
            .await?;
        api_result(status, &v)
    }

    async fn retire(&self, cluster_id: &str) -> Result<()> {
        let (status, v) = self
            .call(
                &format!("/admin/v1/clusters/{cluster_id}:retire"),
                &json!({}),
            )
            .await?;
        api_result(status, &v)
    }
}

/// `host:port` out of `http(s)://host:port[/]`.
fn split_endpoint(endpoint: &str) -> Option<(String, u16)> {
    let rest = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))?
        .trim_end_matches('/');
    let (h, p) = rest.rsplit_once(':')?;
    Some((h.to_owned(), p.parse().ok()?))
}

/// The leader a 39504 answer names (`... send the request to the leader,
/// <name>`); `None` for any other answer.
fn not_leader((status, v): &(u16, Value)) -> Option<String> {
    if *status < 300 || v["code"].as_u64() != Some(u64::from(SprigCode::NotLeader.number())) {
        return None;
    }
    let msg = v["message"].as_str()?;
    let (_, rest) = msg.split_once("send the request to the leader, ")?;
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect();
    let name = name.trim_end_matches('.');
    (!name.is_empty()).then(|| name.to_owned())
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

    /// A one-answer-per-connection HTTP server: every request gets
    /// `answer`'s status and body; `hits` counts requests by path.
    async fn fake_controller(
        status: u16,
        body: Value,
    ) -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let hits = Arc::new(std::sync::Mutex::new(Vec::new()));
        let h = Arc::clone(&hits);
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // Headers, then Content-Length bytes of body.
                let (head_end, len) = loop {
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break (0, 0);
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_ascii_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (i + 4, len);
                    }
                };
                while head_end > 0 && buf.len() < head_end + len {
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let line = String::from_utf8_lossy(&buf);
                let path = line.split(' ').nth(1).unwrap_or("").to_owned();
                h.lock().unwrap().push(path);
                let b = body.to_string();
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{b}",
                    b.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            }
        });
        (port, hits)
    }

    /// The Service lands on a non-leader, which answers 39504 naming the
    /// leader; the registrar sends the call again to the leader's address
    /// from the template, and later calls go there first (before,
    /// registration got through only when a reconcile
    /// pass happened to land on the leader).
    #[tokio::test]
    async fn leader_only_calls_follow_the_dc_leader() {
        let (leader_port, leader_hits) = fake_controller(
            200,
            json!({"cluster": {"cluster_id": "kind1-c1", "cluster_number": 1}}),
        )
        .await;
        let (svc_port, svc_hits) = fake_controller(
            503,
            json!({"code": 39504, "message":
                "this controller doesn't lead dc kind1; send the request to the leader, 127.0.0.1"}),
        )
        .await;
        let r = ClusterRegistration {
            name: "c1".into(),
            topology: TopologyKind::SingleNode,
            nodes: vec![],
            secrets: SecretRefs {
                ctl: "file:/x#a".into(),
                repl: "file:/x#b".into(),
            },
            failover_driver: FailoverDriver::Operator,
        };
        // Without a template the refusal is returned (retried next pass).
        let plain = AdminRegistrar::new(&format!("http://127.0.0.1:{svc_port}"), None).unwrap();
        let e = plain.register(&r).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::NotLeader));
        assert_eq!(leader_hits.lock().unwrap().len(), 0);

        let a = AdminRegistrar::new(&format!("http://127.0.0.1:{svc_port}"), None)
            .unwrap()
            .with_leader_template(Some(format!("http://{{leader}}:{leader_port}")))
            .unwrap();
        let got = a.register(&r).await.unwrap();
        assert_eq!(got.cluster_id, "kind1-c1");
        assert_eq!(svc_hits.lock().unwrap().len(), 2);
        assert_eq!(leader_hits.lock().unwrap().len(), 1);
        // The leader is remembered: the next call goes straight to it.
        a.set_nodes("kind1-c1", &[]).await.unwrap();
        assert_eq!(svc_hits.lock().unwrap().len(), 2);
        assert_eq!(
            leader_hits.lock().unwrap().last().map(String::as_str),
            Some("/admin/v1/clusters/kind1-c1:setNodes")
        );
    }

    #[test]
    fn leader_names_and_templates() {
        let nl = |m: &str| (503, json!({"code": 39504, "message": m}));
        assert_eq!(
            not_leader(&nl(
                "this controller doesn't lead dc fra1; send the request to the leader, sprigdb-controller-2"
            )),
            Some("sprigdb-controller-2".into())
        );
        assert_eq!(
            not_leader(&nl("dc fra1 has no leader yet; try again")),
            None
        );
        assert_eq!(
            not_leader(&(
                400,
                json!({"code": 39511, "message": "send the request to the leader, x"})
            )),
            None
        );
        let a = AdminRegistrar::new("http://ctl.ns.svc:7480", None).unwrap();
        assert!(
            a.clone()
                .with_leader_template(Some("http://ctl:7480".into()))
                .is_err()
        );
        assert!(
            a.clone()
                .with_leader_template(Some("https://{leader}.p.ns.svc:7480".into()))
                .is_err()
        );
        let a = a
            .with_leader_template(Some(
                "http://{leader}.sprigdb-controller-pods.ns.svc:7480".into(),
            ))
            .unwrap();
        assert_eq!(
            a.leader_addr("sprigdb-controller-1"),
            Some((
                "sprigdb-controller-1.sprigdb-controller-pods.ns.svc".into(),
                7480
            ))
        );
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
