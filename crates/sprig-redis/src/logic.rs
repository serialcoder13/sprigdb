//! `FCALL` and `FCALL_RO` of logic-layer functions.
//!
//! A name `FCALL` doesn't find among the tenant's Lua library functions
//! goes to the logic layer: Lua functions are found first. The gateway installs the [`LogicCalls`] hook
//! on the [`crate::Env`]; it turns the call into the logic layer's
//! function call statement on the login's own SQL session, so `EXECUTE`, the
//! function's `EXPOSE (redis)`, its tier, budgets and transaction are
//! those of every other caller. Without a hook the name is simply not
//! found, as in Redis.

use async_trait::async_trait;
use bytes::Bytes;
use sprig_kv::SqlRunner;
use sprig_proto_redis::Frame;

/// One `FCALL` of a logic function.
#[derive(Debug, Clone)]
pub struct Fcall<'a> {
    /// The name as sent: `name`, or `db.name`.
    pub name: &'a [u8],
    /// `KEYS`: they bind the leading parameters.
    pub keys: &'a [Bytes],
    /// `ARGV`: the parameters after the keys.
    pub args: &'a [Bytes],
    /// `FCALL_RO`.
    pub read_only: bool,
    /// The tenant's `redis.logical_database`, where a plain name is
    /// looked up.
    pub database: &'a str,
}

/// Calls logic functions for `FCALL` (the gateway's implementation).
#[async_trait]
pub trait LogicCalls: Send + Sync + std::fmt::Debug {
    /// Runs `call` through `sql`, the login's session. The reply is
    /// `-ERR Function not found` when there is no such function, so a
    /// client can't tell where the name was missing.
    async fn fcall(&self, sql: &mut dyn SqlRunner, call: Fcall<'_>) -> Frame;
}
