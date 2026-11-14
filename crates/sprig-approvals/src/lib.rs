//! `sprig-approvals`: approvals and recommendations for every automatic
//! decision.
//!
//! Layer 2.
//!
//! - [`model`]: the recommendation, kinds, levels, states, ids;
//! - [`policy`]: a kind's mode, level and timers at one subject;
//! - [`machine`]: the state machine as pure functions;
//! - [`access`]: who may act;
//! - [`store`], [`catalog`], [`etcd`]: the memory, tenant-catalog and etcd
//!   stores;
//! - [`gate`]: [`Approvals`], the gate components call before acting,
//!   people's actions, timers and events;
//! - [`events`]: the JSON every notification channel sends;
//! - [`settings`]: the `approvals.*` keys and three new mode keys;
//! - [`smtp`]: a small SMTP client for approval emails;
//! - [`mod@metrics`]: the `sprig_approvals_*` metrics.

pub mod access;
pub mod catalog;
pub mod etcd;
pub mod events;
pub mod gate;
pub mod machine;
pub mod metrics;
pub mod model;
pub mod policy;
pub mod settings;
pub mod smtp;
pub mod store;

pub use access::{Caller, GrantClaim, may_act, may_see};
pub use gate::{ActKnobs, Approvals, Event, Hook, LocatorStores, MemStores, Stores};
pub use machine::{Act, EventKind, Ticket, Verdict};
pub use model::{
    Action, Kind, Level, Mode, NotifyLevel, Proposal, RecId, Recommendation, State, Subject,
};
pub use policy::Policy;
pub use store::{Filter, RecStore};

#[cfg(test)]
mod sim_tests;
