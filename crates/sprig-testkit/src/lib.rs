//! `sprig-testkit`: the test harness, deterministic simulation, differential testing and Compose fixtures.
//!
//! Layer: dev only. Design: [chapter 46](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/46-testing.md) (Testing) of the SprigDB low-level design, with the Compose
//! setup from chapter 27 ("Development setup").
//!
//! Build steps 1 to 3 of chapter 46:
//!
//! - [`env`](mod@env): [`Env`], a description of a test environment (data centers,
//!   MariaDB clusters with their topology and endpoints, etcd tiers), read
//!   from `SPRIG_TEST_*` variables or a TOML file;
//! - [`skip`]: the `SPRIG_TEST_*` convention and [`require_env!`], which
//!   skips a test cleanly when its environment isn't configured;
//! - [`compose`]: Docker Compose fixtures that start a project, wait for
//!   health, collect logs on failure and tear down on drop;
//! - [`util`]: unique names, temporary directories and retry with a timeout;
//! - [`sim`]: deterministic simulation: seeded runs on a paused-clock
//!   runtime, a simulated network, etcd ([`sim::SimEtcd`]) and clocks,
//!   fault plans that shrink on failure, and the MariaDB model with its
//!   conformance scenarios ([`sim::mariadb`]);
//! - [`faults`]: the [`faults::Faults`] trait, toxiproxy and `faultd` for
//!   Compose, throwaway `sprig-sim-*` containers, operation histories and
//!   `elle-cli`;
//! - [`fleet`]: the fleet simulator of chapter 11 (placement and
//!   rebalancing over months of synthetic load, with a fake mover).
//!
//! Differential testing and the other suites of chapter 46 come in its
//! later build steps.
//!
//! # Using it from another crate
//!
//! Add `sprig-testkit` as a dev-dependency, then gate each integration test
//! on its variable:
//!
//! ```no_run
//! # async fn example() {
//! use sprig_testkit::{require_env, skip};
//!
//! // Returns from the test with a "skipped" message unless SPRIG_TEST_ETCD is set.
//! let endpoints = require_env!(skip::ETCD);
//! # let _ = endpoints;
//! # }
//! ```

pub mod compose;
pub mod connectors;
pub mod env;
pub mod faults;
pub mod fleet;
pub mod sim;
pub mod skip;
pub mod util;

pub use compose::{ComposeProject, ComposeStack};
pub use env::{ClusterEnv, DcEnv, Env, Link, Topology};
pub use util::{Retry, TempDir, unique_ident, unique_name};
