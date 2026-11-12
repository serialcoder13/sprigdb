//! Automatic indexing (chapter 13): the decision logic that turns the
//! workload into secondary indexes, run by the controller.
//!
//! What exists:
//!
//! - [`model`]: definitions, origins, candidates and the canonical form
//!   that names an index `sprig_auto_<8 hex>`;
//! - [`store`]: the catalog tables as the `autoindex` migration module,
//!   typed rows and access, the state machine and the decision hook
//!   (step 1);
//! - [`settings`]: every `auto_index.*` key in the registry;
//! - [`stats`]: MariaDB's engine-independent statistics, the `JSON_HB`
//!   histogram parser, selectivity estimates and the throttled analyze job
//!   (step 3);
//! - [`candidates`]: candidate generation, covering, merging and the
//!   coverage check against existing indexes with its `EXPLAIN` probe
//!   (step 4);
//! - [`cost`]: the cost model, the gates, selection under budgets and
//!   replacement (step 5);
//! - [`policy`]: per-object setting resolution, budgets at each scope,
//!   bound tightening and the churn dampers (step 16);
//! - [`replay`] and [`validate`]: the replay harness and the validators
//!   (steps 6-9);
//! - [`rollout`]: online builds, windows, slots, lock-safe retries,
//!   aborts, the Galera method until the mover exists, and the rollout
//!   state machine (step 10);
//! - [`observe`]: the watch, usage tracking, retire and drop, and the drift
//!   reconciler (step 11).
//!
//! - [`qualify`]: trigger evaluation, the fast scan trigger included
//!   (steps 2 and 22);
//! - [`evaluate`]: one evaluation of one tenant, from the workload window
//!   to candidates routed to validation or recommendations, paced by
//!   `auto_index.eval_interval_minutes`.

pub mod candidates;
pub mod cost;
pub mod doc;
pub mod evaluate;
#[cfg(test)]
mod fixtures;
pub mod kv;
pub mod model;
pub mod observe;
pub mod policy;
pub mod qualify;
pub mod replay;
pub mod rollout;
pub mod settings;
pub mod stats;
pub mod store;
pub mod validate;
