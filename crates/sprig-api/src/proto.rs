//! The generated protobuf code (chapter 37 step 1). Do not edit `src/gen/`
//! by hand: change the `.proto` files under `proto/` and run
//! `SPRIG_API_REGEN=1 cargo test -p sprig-api --test codegen`.

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::pedantic,
    clippy::all,
    rust_2018_idioms
)]

/// `sprig.v1`: the data services (`Sql`, `Transactions` in R0).
pub mod v1 {
    include!("gen/sprig.v1.rs");
}

/// `sprig.admin.v1`: the Admin API.
pub mod admin {
    include!("gen/sprig.admin.v1.rs");
}

/// `sprig.internal.v1`: the gateways' peer service on 7403.
pub mod internal {
    include!("gen/sprig.internal.v1.rs");
}

/// `grpc.health.v1`: the standard health service.
pub mod health {
    include!("gen/grpc.health.v1.rs");
}

/// `google.rpc`: `Status` and the error details.
pub mod rpc {
    include!("gen/google.rpc.rs");
}

/// `google.api`: `HttpRule` (only for completeness; routes live in
/// [`crate::rest::ROUTES`]).
pub mod google_api {
    include!("gen/google.api.rs");
}

/// The serialized `FileDescriptorSet` of every proto, for gRPC reflection
/// and tools.
pub const DESCRIPTOR_SET: &[u8] = include_bytes!("gen/descriptor.bin");
