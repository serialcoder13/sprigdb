//! What every connection of a gateway shares: settings, the pub/sub hub,
//! the wait table and which namespaces' tables are known to exist.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::fabric::{Interest, Mesh, PublishMsg};
use crate::pubsub::{Hub, Waits};

/// The settings a connection reads.
#[derive(Debug, Clone)]
pub struct Config {
    /// `redis.server_version`.
    pub server_version: String,
    /// `redis.databases`.
    pub databases: u32,
    /// `redis.logical_database`.
    pub logical_database: String,
    /// `redis.max_multi_commands`.
    pub max_multi_commands: usize,
    /// `redis.write_retries`.
    pub write_retries: u32,
    /// `redis.block_poll_ms`.
    pub block_poll: Duration,
    /// `redis.max_blocked_per_tenant`.
    pub max_blocked_per_tenant: usize,
    /// `redis.scan_small_key_elements`.
    pub scan_small_key_elements: usize,
    /// `redis.lua_time_limit_ms`.
    pub lua_time_limit: Duration,
    /// `redis.lua_memory_limit_mb`, in bytes.
    pub lua_memory_bytes: usize,
    /// `redis.lua_kill_after_ms`.
    pub lua_kill_after: Duration,
    /// `redis.lua_undeclared_keys`: `false` refuses keys not in `KEYS`.
    pub lua_undeclared_same_shard: bool,
    /// `redis.max_bulk_bytes`.
    pub max_bulk_bytes: usize,
    /// `redis.max_command_bytes`.
    pub max_command_bytes: usize,
    /// `redis.idle_timeout_seconds` (0 = never).
    pub idle_timeout: Duration,
    /// `redis.client_output_buffer_pubsub_mb`, in bytes.
    pub pubsub_buffer_bytes: usize,
    /// `redis.guarded_batches`: the hottest writes run as one compound
    /// statement ([`crate::batch`]); `false` keeps every command on the
    /// statement path.
    pub guarded_batches: bool,
    /// `redis.notify_keyspace_events`: Redis's flag string; empty sends no
    /// keyspace notifications ([`crate::keyspace`]).
    pub notify_keyspace_events: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_version: "7.2.0".into(),
            databases: 16,
            logical_database: "redis".into(),
            max_multi_commands: 10_000,
            write_retries: 10,
            block_poll: Duration::from_millis(1000),
            max_blocked_per_tenant: 10_000,
            scan_small_key_elements: 128,
            lua_time_limit: Duration::from_millis(5000),
            lua_memory_bytes: 32 << 20,
            lua_kill_after: Duration::from_millis(1000),
            lua_undeclared_same_shard: false,
            max_bulk_bytes: 16 << 20,
            max_command_bytes: 64 << 20,
            idle_timeout: Duration::ZERO,
            pubsub_buffer_bytes: 32 << 20,
            guarded_batches: true,
            notify_keyspace_events: String::new(),
        }
    }
}

/// A gateway's shared Redis state.
#[derive(Debug)]
pub struct Env {
    /// Settings now (the gateway refreshes them).
    pub config: Mutex<Config>,
    /// Pub/sub channels of every tenant on this gateway.
    pub hub: Hub,
    /// Blocked clients.
    pub waits: Waits,
    /// `<tenant>/<database>/<table>` whose tables exist.
    ensured: Mutex<HashSet<String>>,
    /// Connection ids.
    next_id: AtomicU64,
    /// Process start, for `INFO`.
    pub started: Instant,
    /// Gateway id, for `INFO` and `CLIENT LIST`.
    pub gateway_id: String,
    /// Running scripts, for `SCRIPT KILL` and `FUNCTION KILL`.
    pub scripts: crate::lua::Running,
    /// The hubs across gateways; unset, pub/sub and wake-ups
    /// stay on this gateway.
    mesh: OnceLock<Arc<Mesh>>,
    /// `FCALL` of logic-layer functions; unset, only Lua
    /// library functions are found.
    logic: OnceLock<Arc<dyn crate::logic::LogicCalls>>,
    /// `CONFIG SET notify-keyspace-events` per tenant on this gateway,
    /// over [`Config::notify_keyspace_events`].
    keyspace: Mutex<std::collections::HashMap<String, String>>,
    /// Who may see a keyspace notification (row-level security); unset,
    /// every subscriber does.
    key_filter: OnceLock<Arc<dyn crate::keyspace::KeyFilter>>,
    /// The notifications from the change feed; unset, none.
    keyspace_feed: OnceLock<Arc<dyn crate::keyspace::KeyspaceFeed>>,
}

impl Default for Env {
    fn default() -> Self {
        Self::new(Config::default(), "sprig")
    }
}

impl Env {
    /// Shared state with `config`.
    #[must_use]
    pub fn new(config: Config, gateway_id: &str) -> Self {
        Self {
            config: Mutex::new(config),
            hub: Hub::default(),
            waits: Waits::default(),
            ensured: Mutex::new(HashSet::new()),
            next_id: AtomicU64::new(1),
            started: Instant::now(),
            gateway_id: gateway_id.to_owned(),
            scripts: crate::lua::Running::default(),
            mesh: OnceLock::new(),
            logic: OnceLock::new(),
            keyspace: Mutex::new(std::collections::HashMap::new()),
            key_filter: OnceLock::new(),
            keyspace_feed: OnceLock::new(),
        }
    }

    /// The keyspace notification flags of tenant `scope` (canonical).
    #[must_use]
    pub fn keyspace_flags(&self, scope: &str) -> String {
        let own = self
            .keyspace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(scope)
            .cloned();
        own.unwrap_or_else(|| self.config().notify_keyspace_events)
    }

    /// `CONFIG SET notify-keyspace-events` for tenant `scope` on this
    /// gateway.
    pub fn set_keyspace_flags(&self, scope: &str, flags: String) {
        self.keyspace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope.to_owned(), flags);
    }

    /// Installs the keyspace notifications' privilege and row-level check
    /// (the gateway does, once).
    pub fn set_key_filter(&self, f: Arc<dyn crate::keyspace::KeyFilter>) {
        let _ = self.key_filter.set(f);
    }

    /// The check, when installed.
    #[must_use]
    pub fn key_filter(&self) -> Option<&Arc<dyn crate::keyspace::KeyFilter>> {
        self.key_filter.get()
    }

    /// Installs the notifications from the change feed (the gateway does,
    /// once).
    pub fn set_keyspace_feed(&self, f: Arc<dyn crate::keyspace::KeyspaceFeed>) {
        let _ = self.keyspace_feed.set(f);
    }

    /// The notifications from the change feed, when installed.
    #[must_use]
    pub fn keyspace_feed(&self) -> Option<&Arc<dyn crate::keyspace::KeyspaceFeed>> {
        self.keyspace_feed.get()
    }

    /// Installs the logic layer's `FCALL` hook (the gateway does, once).
    pub fn set_logic(&self, l: Arc<dyn crate::logic::LogicCalls>) {
        let _ = self.logic.set(l);
    }

    /// The logic layer's `FCALL` hook, when installed.
    #[must_use]
    pub fn logic(&self) -> Option<&Arc<dyn crate::logic::LogicCalls>> {
        self.logic.get()
    }

    /// Joins the gateways' hubs ([`Mesh::attach`] calls this).
    pub fn set_mesh(&self, m: Arc<Mesh>) {
        let _ = self.mesh.set(m);
    }

    /// The hubs, when this gateway has them.
    #[must_use]
    pub fn mesh(&self) -> Option<&Arc<Mesh>> {
        self.mesh.get()
    }

    /// `PUBLISH` / `SPUBLISH`: through the hub, or on this gateway alone.
    ///
    /// # Errors
    /// No hub reachable: the reason.
    pub async fn publish(
        &self,
        scope: &str,
        channel: &Bytes,
        data: &Bytes,
        shard: bool,
    ) -> Result<i64, String> {
        match self.mesh() {
            Some(m) => {
                m.publish(PublishMsg {
                    scope: scope.to_owned(),
                    channel: channel.clone(),
                    data: data.clone(),
                    shard,
                })
                .await
            }
            None => Ok(self.hub.publish(scope, channel, data, shard)),
        }
    }

    /// Registers this gateway's interest with the hub (no-op alone).
    pub async fn hold(&self, scope: &str, i: Interest) {
        if let Some(m) = self.mesh()
            && let Err(e) = m.hold(scope, i).await
        {
            tracing::debug!(error = %e, "redis hub unreachable; the next refresh registers the interest");
        }
    }

    /// Drops this gateway's interest (no-op alone).
    pub fn release(&self, scope: &str, i: Interest) {
        if let Some(m) = self.mesh() {
            m.release(scope, i);
        }
    }

    /// Wakes clients blocked on `key`: here at once, on other gateways
    /// through the hub.
    pub fn wake(&self, scope: &str, db: u32, key: &[u8]) {
        self.wake_many(scope, db, std::iter::once(key));
    }

    /// [`Self::wake`] for several keys of one database.
    pub fn wake_many<'k>(&self, scope: &str, db: u32, keys: impl IntoIterator<Item = &'k [u8]>) {
        let mut remote = Vec::new();
        for k in keys {
            self.waits.wake(scope, db, k);
            remote.push(Bytes::copy_from_slice(k));
        }
        if let Some(m) = self.mesh() {
            m.wake(scope, db, remote);
        }
    }

    /// The settings now.
    #[must_use]
    pub fn config(&self) -> Config {
        self.config
            .lock()
            .map_or_else(|p| p.into_inner().clone(), |c| c.clone())
    }

    /// Replaces the settings.
    pub fn set_config(&self, c: Config) {
        if let Ok(mut g) = self.config.lock() {
            *g = c;
        }
    }

    /// A new connection id.
    pub fn conn_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Whether `key`'s tables are known to exist.
    #[must_use]
    pub fn is_ensured(&self, key: &str) -> bool {
        self.ensured.lock().is_ok_and(|s| s.contains(key))
    }

    /// Records that `key`'s tables exist.
    pub fn set_ensured(&self, key: &str) {
        if let Ok(mut s) = self.ensured.lock() {
            s.insert(key.to_owned());
        }
    }

    /// Forgets `key` (its tables were found missing).
    pub fn forget_ensured(&self, key: &str) {
        if let Ok(mut s) = self.ensured.lock() {
            s.remove(key);
        }
    }
}
