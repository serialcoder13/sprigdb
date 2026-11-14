//! Error mapping.
//!
//! Every error leaves the HTTP listeners in one of two forms, built from the
//! same [`SprigError`]:
//!
//! - **gRPC:** a `Status` whose code comes from the error's protocol
//!   mapping ([`SprigError::protocol`]), whose message is MySQL's text, and whose
//!   `grpc-status-details-bin` holds a `google.rpc.Status` with an
//!   `ErrorInfo` (`reason`: the `ER_SPRIG_*` name, or `MYSQL_ERROR` for a
//!   MariaDB or standard error; metadata `mysql_errno`, `sqlstate` and, for
//!   SprigDB codes, `sprig_code`) and, for retryable errors, a `RetryInfo`.
//! - **REST:** SprigDB's common error JSON, `{"code", "name", "message",
//!   "sqlstate", "trace_id"}`, with the HTTP status of the same table.
//!
//! Both parse back to the same error ([`from_status`], [`from_rest`]), so a
//! forwarded call or a client sees exactly the error the wire would.

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use sprig_common::{ErrorKind, SprigCode, SprigError};

use crate::proto::rpc;

/// `ErrorInfo.domain`.
pub const DOMAIN: &str = "sprigdb.io";
/// `ErrorInfo.reason` of an error that isn't a SprigDB code.
pub const MYSQL_REASON: &str = "MYSQL_ERROR";
/// How long `RetryInfo` asks a client to wait before retrying.
pub const RETRY_DELAY_MS: u64 = 100;

const ERROR_INFO_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";
const RETRY_INFO_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// The MySQL error number of `e`.
#[must_use]
pub fn errno(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

/// `ER_SPRIG_*` for SprigDB codes, [`MYSQL_REASON`] otherwise.
#[must_use]
pub fn reason(e: &SprigError) -> &'static str {
    match e.kind {
        ErrorKind::Sprig(c) => c.name(),
        _ => MYSQL_REASON,
    }
}

fn sqlstate(e: &SprigError) -> String {
    String::from_utf8_lossy(&e.protocol().sqlstate).into_owned()
}

/// The gRPC status for `e`.
#[must_use]
pub fn to_status(e: &SprigError) -> tonic::Status {
    let p = e.protocol();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("mysql_errno".to_owned(), p.mysql_errno.to_string());
    metadata.insert("sqlstate".to_owned(), sqlstate(e));
    if let ErrorKind::Sprig(c) = e.kind {
        metadata.insert("sprig_code".to_owned(), c.number().to_string());
    }
    let info = rpc::ErrorInfo {
        reason: reason(e).to_owned(),
        domain: DOMAIN.to_owned(),
        metadata,
    };
    let mut details = vec![prost_types::Any {
        type_url: ERROR_INFO_URL.to_owned(),
        value: info.encode_to_vec(),
    }];
    if e.retryable {
        let retry = rpc::RetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: 0,
                nanos: i32::try_from(RETRY_DELAY_MS * 1_000_000).unwrap_or(0),
            }),
        };
        details.push(prost_types::Any {
            type_url: RETRY_INFO_URL.to_owned(),
            value: retry.encode_to_vec(),
        });
    }
    let code = p.grpc_status;
    let status = rpc::Status {
        code: code as i32,
        message: e.message.clone(),
        details,
    };
    tonic::Status::with_details(code, e.message.clone(), Bytes::from(status.encode_to_vec()))
}

/// The error a gRPC status carries: exact when it has SprigDB's
/// `ErrorInfo`, otherwise an `Internal` error with the status text.
#[must_use]
pub fn from_status(s: &tonic::Status) -> SprigError {
    let info = rpc::Status::decode(s.details())
        .ok()
        .into_iter()
        .flat_map(|st| st.details)
        .find(|a| a.type_url == ERROR_INFO_URL)
        .and_then(|a| rpc::ErrorInfo::decode(a.value.as_slice()).ok());
    let Some(info) = info else {
        return SprigError::internal(format!("{:?}: {}", s.code(), s.message()));
    };
    let errno = info
        .metadata
        .get("mysql_errno")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let state = info
        .metadata
        .get("sqlstate")
        .map_or("HY000", String::as_str);
    build(errno, info.reason.as_str(), state, s.message())
}

fn sqlstate_bytes(s: &str) -> [u8; 5] {
    let mut out = *b"HY000";
    if s.len() == 5 && s.is_ascii() {
        out.copy_from_slice(s.as_bytes());
    }
    out
}

/// The error a MySQL ERR packet's parts describe: a SprigDB code when the
/// number is one, otherwise a MariaDB or standard error. Front ends that
/// render through a persona's error map ([`ErrPacket::from_error`]) send its
/// output through here, so HTTP shows exactly the wire's number, SQLSTATE
/// and text.
///
/// [`ErrPacket::from_error`]: https://docs.rs/sprig-proto-mysql
#[must_use]
pub fn from_parts(errno: u16, sqlstate: &[u8; 5], message: &str) -> SprigError {
    build(errno, "", &String::from_utf8_lossy(sqlstate), message)
}

fn build(errno: u16, reason: &str, state: &str, message: &str) -> SprigError {
    if let Some(code) = SprigCode::ALL.iter().find(|c| c.number() == errno) {
        return SprigError::new(*code, message);
    }
    let _ = reason;
    // A standard or MariaDB error: the wire shows both the same way.
    SprigError::backend(errno, sqlstate_bytes(state), message)
}

/// SprigDB's REST error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// The MySQL error number (39000-39999 for SprigDB's own).
    pub code: u16,
    /// `ER_SPRIG_*`, or [`MYSQL_REASON`].
    pub name: String,
    /// MySQL's message text.
    pub message: String,
    /// The SQLSTATE.
    #[serde(default)]
    pub sqlstate: String,
    /// The request's trace id, when tracing knows one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Safe to retry.
    #[serde(default)]
    pub retryable: bool,
}

/// The REST status and body for `e`.
#[must_use]
pub fn to_rest(e: &SprigError) -> (u16, ErrorBody) {
    let p = e.protocol();
    (
        p.http_status,
        ErrorBody {
            code: p.mysql_errno,
            name: reason(e).to_owned(),
            message: e.message.clone(),
            sqlstate: sqlstate(e),
            trace_id: None,
            retryable: e.retryable,
        },
    )
}

/// The error a REST error body describes.
#[must_use]
pub fn from_rest(body: &ErrorBody) -> SprigError {
    build(body.code, &body.name, &body.sqlstate, &body.message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<SprigError> {
        vec![
            SprigError::backend(1146, *b"42S02", "Table 'shop.nope' doesn't exist"),
            SprigError::backend(1062, *b"23000", "Duplicate entry '1' for key 'PRIMARY'"),
            SprigError::standard(1213, *b"40001", "Deadlock found"),
            SprigError::new(SprigCode::ReservedName, "reserved"),
            SprigError::new(SprigCode::TxnHandleExpired, "expired"),
            SprigError::new(SprigCode::GatewayDraining, "draining"),
        ]
    }

    #[test]
    fn grpc_status_round_trips() {
        for e in samples() {
            let s = to_status(&e);
            assert_eq!(s.code(), e.protocol().grpc_status);
            let back = from_status(&s);
            assert_eq!(back.protocol(), e.protocol(), "{e}");
            assert_eq!(back.message, e.message);
            assert_eq!(back.retryable, e.retryable);
        }
    }

    #[test]
    fn rest_body_round_trips() {
        for e in samples() {
            let (http, body) = to_rest(&e);
            assert_eq!(http, e.protocol().http_status);
            let json = serde_json::to_string(&body).unwrap();
            let parsed: ErrorBody = serde_json::from_str(&json).unwrap();
            let back = from_rest(&parsed);
            assert_eq!(back.protocol(), e.protocol());
            assert_eq!(back.message, e.message);
        }
    }

    #[test]
    fn chapter_37_table() {
        let e = SprigError::backend(1146, *b"42S02", "x");
        assert_eq!(to_status(&e).code(), tonic::Code::NotFound);
        assert_eq!(to_rest(&e).0, 404);
        let e = SprigError::backend(1045, *b"28000", "x");
        assert_eq!(to_rest(&e).0, 401);
        let e = SprigError::standard(1213, *b"40001", "x");
        let s = to_status(&e);
        assert_eq!(s.code(), tonic::Code::Aborted);
        let st = rpc::Status::decode(s.details()).unwrap();
        assert!(st.details.iter().any(|a| a.type_url == RETRY_INFO_URL));
        assert_eq!(
            to_rest(&SprigError::new(SprigCode::ReservedName, "r"))
                .1
                .name,
            "ER_SPRIG_RESERVED_NAME"
        );
    }
}
