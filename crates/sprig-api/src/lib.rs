//! `sprig-api`: protobuf (`sprig.v1`, `sprig.admin.v1`), tonic services and REST transcoding.
//!
//! Layer: 2 front ends.
//!
//! What exists:
//! - [`proto`]: the generated prost messages and tonic services of
//!   `sprig.v1` (`Sql`, `Transactions`), `sprig.admin.v1` (one service per
//!   `sprigctl` command group), `sprig.internal.v1.GatewayPeer` (the
//!   gateways' peer service on 7403), `grpc.health.v1` and the `google.rpc`
//!   error details. The output of `protoc` is checked in under `src/gen/`;
//!   `tests/codegen.rs` regenerates it and fails when it is stale, so a
//!   workspace build needs no `protoc`.
//! - [`rest`]: the REST face. Every RPC's `google.api.http` rule is in one
//!   route table ([`rest::ROUTES`], checked against the protos by a test),
//!   with a path-template matcher and the `OpenAPI` v3 document built from
//!   the table.
//! - [`status`]: error mapping. A [`SprigError`](sprig_common::SprigError)
//!   becomes a gRPC `Status` carrying `google.rpc.Status` with `ErrorInfo`
//!   (and `RetryInfo` when retryable), or SprigDB's REST error JSON with
//!   the HTTP status for its kind; both parse back losslessly.
//! - [`results`]: SQL results in proto form and their JSON (proto3 JSON:
//!   64-bit integers as strings, rows as JSON arrays of strings and nulls).
//! - [`handle`]: transaction handles, base64url of a small protobuf naming
//!   the owner gateway, its session and 128 random bits.
//!
//! The services themselves run in the gateway (`sprig-gateway::http`),
//! which owns sessions, logins and the executor.

pub mod handle;
pub mod proto;
pub mod rest;
pub mod results;
pub mod status;
