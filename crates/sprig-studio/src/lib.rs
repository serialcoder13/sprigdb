//! `sprig-studio`: Studio, SprigDB's web client: its built files, embedded
//! in the binary, and how the gateway serves them. The UI source lives in
//! `studio/` at the repository root.
//!
//! Layer: 3 roles (served by the gateway). Design: [chapter 44](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/44-studio.md)
//! (Studio) of the SprigDB low-level design.
//!
//! - [`assets`]: the embedded files ([`AssetStore`]), written by `build.rs`
//!   from `studio/dist` (`pnpm build`), each with its brotli and gzip
//!   forms and an `ETag`;
//! - [`settings`]: the `studio.*` keys and [`StudioConfig`], the values
//!   serving needs, read live;
//! - [`serve`]: one request under `studio.path` → one response, with the
//!   security headers and Content-Security-Policy on every answer.
//!
//! The gateway's HTTP front end calls [`serve::serve`] for `GET` and `HEAD`
//! requests under `studio.path` and for `POST <path>/_/errors`, ahead of
//! the REST router (chapter 37). Studio itself uses only the public API
//! with the signed-in user's token.

pub mod api;
pub mod assets;
pub mod serve;
pub mod settings;

pub use assets::{Asset, AssetStore};
pub use serve::{StudioRequest, StudioResponse, serve};
pub use settings::StudioConfig;
