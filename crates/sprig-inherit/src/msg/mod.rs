//! Inherited messaging objects (chapter 42, "Inherited messaging objects
//! (`FOR EACH TENANT`)"; R9, build steps 14–19): the machinery every kind
//! shares.
//!
//! - [`model`]: definitions, overrides and instances; membership
//!   ([`Scope::reaches`]) and override resolution ([`Chain::effective`],
//!   nearest level wins);
//! - [`store`]: `inherited_defs`, `inherited_overrides` and
//!   `inherited_instances` in the tenant catalog, and the snapshot loader
//!   [`MsgExtension`];
//! - [`grammar`]: `FOR EACH TENANT`, `FOR TENANT`, `FOR ALL TENANTS`, `IN
//!   TENANT`, `ALTER INHERITED` and `SHOW INHERITED`.
//!
//! The gateway glues them to each kind (`sprig-gateway`'s `inherited`
//! module): it resolves a name in a member through the member's chain of
//! catalogs, creates instances lazily, publishes by target tenant and
//! deploys inherited triggers as the member's `sprig_inherited`.

pub mod grammar;
pub mod model;
pub mod settings;
pub mod store;

pub use model::{
    AppliesTo, BindingSpec, Cause, Chain, DefKey, DefKind, DefState, Definition, Depth,
    ExchangeSpec, GrantItem, Instance, InstanceState, Level, MsgState, Override, QueueSpec, Scope,
    StreamSpec, TriggerSpec, What,
};
pub use store::{MODULE, MSG_MODULE, MsgExtension, MsgStore};

#[cfg(test)]
mod tests;
