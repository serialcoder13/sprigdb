//! The Admin API client (chapter 47, "sprigctl reference"; chapter 37).
//!
//! JSON over HTTP/1.1 to `--endpoint`. Errors come back as `{code, name,
//! message, trace_id}`. `https://` endpoints wait for the gateway's 8443
//! listener; the controller serves plain HTTP on loopback.

use std::fmt;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Uri, header};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_controller::admin::types::ErrorBody;

/// The default endpoint: the controller's Admin API on loopback.
pub(crate) const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7480";

/// How long one call may take; registration probes every node deeply.
const CALL_TIMEOUT: Duration = Duration::from_secs(180);

/// What went wrong with a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallError {
    /// The API refused the call.
    Api(ErrorBody),
    /// The call didn't complete.
    Transport(String),
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Api(e) if e.name.is_empty() => write!(f, "{}: {}", e.code, e.message),
            CallError::Api(e) => write!(f, "{} {}: {}", e.code, e.name, e.message),
            CallError::Transport(m) => f.write_str(m),
        }
    }
}

/// A client of one Admin API endpoint.
#[derive(Debug, Clone)]
pub(crate) struct AdminClient {
    base: String,
    token: Option<String>,
    http: Client<HttpConnector, Full<Bytes>>,
}

impl AdminClient {
    /// A client of `endpoint` (`http://host:port`), sending `token` as a
    /// bearer token when given.
    ///
    /// # Errors
    /// For an endpoint that isn't an `http://` URL.
    pub(crate) fn new(endpoint: &str, token: Option<String>) -> Result<Self, CallError> {
        let base = endpoint.trim_end_matches('/').to_owned();
        let uri: Uri = base
            .parse()
            .map_err(|e| CallError::Transport(format!("endpoint {endpoint:?}: {e}")))?;
        match uri.scheme_str() {
            Some("http") => {}
            Some("https") => {
                return Err(CallError::Transport(format!(
                    "endpoint {endpoint}: https needs the gateway's 8443 listener, which \
                     doesn't serve the Admin API yet; use the controller's http:// address"
                )));
            }
            _ => {
                return Err(CallError::Transport(format!(
                    "endpoint {endpoint:?}: expected http://host:port"
                )));
            }
        }
        Ok(Self {
            base,
            token,
            http: Client::builder(TokioExecutor::new()).build_http(),
        })
    }

    /// `GET path`.
    ///
    /// # Errors
    /// The API's error, or a transport failure.
    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, CallError> {
        self.call(Method::GET, path, None).await
    }

    /// `POST path` with `body` as JSON.
    ///
    /// # Errors
    /// The API's error, or a transport failure.
    pub(crate) async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CallError> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| CallError::Transport(format!("encoding the request: {e}")))?;
        self.call(Method::POST, path, Some(bytes)).await
    }

    /// `PUT path` with `body` as JSON.
    ///
    /// # Errors
    /// The API's error, or a transport failure.
    pub(crate) async fn put<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CallError> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| CallError::Transport(format!("encoding the request: {e}")))?;
        self.call(Method::PUT, path, Some(bytes)).await
    }

    /// `DELETE path`.
    ///
    /// # Errors
    /// The API's error, or a transport failure.
    pub(crate) async fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, CallError> {
        self.call(Method::DELETE, path, None).await
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, CallError> {
        let url = format!("{}{path}", self.base);
        let mut b = Request::builder()
            .method(method)
            .uri(&url)
            .header(header::ACCEPT, "application/json");
        if body.is_some() {
            b = b.header(header::CONTENT_TYPE, "application/json");
        }
        if let Some(t) = &self.token {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let req = b
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|e| CallError::Transport(format!("{url}: {e}")))?;
        let resp = tokio::time::timeout(CALL_TIMEOUT, self.http.request(req))
            .await
            .map_err(|_| CallError::Transport(format!("{url}: no answer within {CALL_TIMEOUT:?}")))?
            .map_err(|e| CallError::Transport(format!("{url}: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| CallError::Transport(format!("{url}: {e}")))?
            .to_bytes();
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .map_err(|e| CallError::Transport(format!("{url}: unexpected answer: {e}")));
        }
        match serde_json::from_slice::<ErrorBody>(&bytes) {
            Ok(e) => Err(CallError::Api(e)),
            Err(_) => Err(CallError::Transport(format!(
                "{url}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            ))),
        }
    }
}
