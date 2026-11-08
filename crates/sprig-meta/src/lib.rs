//! `sprig-meta`: the `MetaStore` trait, the only way to etcd.
//!
//! Layer 1 (foundations). Design: chapter 23 (Control plane) of the SprigDB
//! low-level design, with the key layout in Appendix D.
//!
//! Consensus is needed only for a few facts (shard owners, leaders, leases,
//! jobs), and etcd provides them. Everything goes through the small
//! [`MetaStore`] trait so that openraft could replace etcd later.
//!
//! What exists:
//! - [`MetaStore`] and its types (step 1);
//! - [`MemStore`]: deterministic, in memory, with revisions, history,
//!   leases on an injectable clock, watches and fault injection (step 1);
//! - [`Tier`] and [`Key`] for the key layout, and versioned [`Record`]s
//!   (step 1);
//! - [`EtcdStore`]: paging, transactions, watches, leases and mutual TLS
//!   (step 2), with single-etcd mode ([`etcd::connect_tiers`]);
//! - [`session`] and [`elect`]: lease-backed sessions, election with
//!   fencing tokens and guarded transactions (step 3);
//! - [`bootstrap`]: the first DC's deployment records (step 7, this
//!   crate's part);
//! - [`cache`]: full ([`Mirror`]) and lazy ([`LazyCache`]) caches fed by
//!   watches (step 4);
//! - `conformance` (feature `conformance`): one suite every store must
//!   pass, run here against `MemStore` and, when `SPRIG_TEST_ETCD` names an
//!   etcd, against `EtcdStore`.
//!
//! Not yet: discovery (step 9).

pub mod bootstrap;
pub mod cache;
pub mod discovery;
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
pub mod elect;
pub mod etcd;
pub mod key;
pub mod mem;
pub mod record;
pub mod session;
pub mod view;

use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use sprig_common::Result;

pub use cache::{LazyCache, Mirror};
pub use elect::{Election, Elector, Leader, LeaderRecord, Leadership};
pub use etcd::{EtcdConfig, EtcdStore, EtcdTls};
pub use key::{Key, Tier};
pub use mem::MemStore;
pub use record::{Record, Versioned};
pub use session::{Session, SessionOpts};
pub use view::TierView;

/// An etcd revision: the store's logical clock, raised by every write.
pub type Revision = u64;

/// A lease id, as etcd assigns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseId(pub i64);

/// How fresh a read must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Consistency {
    /// Goes through the leader: sees every committed write.
    #[default]
    Linearizable,
    /// Served by any member: may be slightly stale, cheaper.
    Serializable,
}

/// A stored key with its revisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    /// The key.
    pub key: Key,
    /// The value: a JSON record ([`Record`]).
    pub value: Vec<u8>,
    /// Revision that created the key (since its last deletion).
    pub create_revision: Revision,
    /// Revision of the last change.
    pub mod_revision: Revision,
    /// Number of changes since creation, starting at 1.
    pub version: u64,
    /// The lease the key is bound to, if any.
    pub lease: Option<LeaseId>,
}

/// Options for [`MetaStore::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOpts {
    /// Read at this revision; `None` reads the current one. Pin every page
    /// of one listing to the first page's revision.
    pub at: Option<Revision>,
    /// Start after this key (the last key of the previous page).
    pub after: Option<Key>,
    /// Page size.
    pub limit: usize,
}

impl Default for ListOpts {
    fn default() -> Self {
        Self {
            at: None,
            after: None,
            limit: 500,
        }
    }
}

/// One page of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    /// Keys in order.
    pub kvs: Vec<KeyValue>,
    /// The revision the page was read at.
    pub revision: Revision,
    /// More keys follow; ask again with `after` = the last key.
    pub more: bool,
}

/// A condition of a [`Txn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmp {
    /// The key doesn't exist.
    Missing(Key),
    /// The key exists with this create revision.
    CreateRev(Key, Revision),
    /// The key exists with this mod revision.
    ModRev(Key, Revision),
}

/// An operation of a [`Txn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Writes a value, optionally bound to a lease.
    Put {
        /// The key.
        key: Key,
        /// The value.
        value: Vec<u8>,
        /// Delete the key when this lease ends.
        lease: Option<LeaseId>,
    },
    /// Deletes a key.
    Delete(Key),
    /// Reads a key.
    Get(Key),
}

/// The result of one [`Op`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// The put happened.
    Put,
    /// Whether a key was deleted.
    Delete(bool),
    /// The key's value, if it exists.
    Get(Option<KeyValue>),
}

/// An atomic compare-and-act: if every `when` holds, run `then`, else run
/// `otherwise`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Txn {
    /// Conditions; all must hold.
    pub when: Vec<Cmp>,
    /// Operations if they hold.
    pub then: Vec<Op>,
    /// Operations if they don't.
    pub otherwise: Vec<Op>,
}

/// What a [`Txn`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnResult {
    /// `true` if `then` ran.
    pub succeeded: bool,
    /// The store's revision after the transaction.
    pub revision: Revision,
    /// One result per operation run.
    pub results: Vec<OpResult>,
}

/// One event of a watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// A key was written.
    Put(KeyValue),
    /// A key was deleted.
    Delete {
        /// The key.
        key: Key,
        /// The revision of the deletion.
        mod_revision: Revision,
    },
    /// Every event up to this revision has been delivered.
    Progress(Revision),
    /// The requested start revision is compacted: list again, then watch
    /// from the listing's revision + 1. The stream ends after this event.
    Compacted {
        /// The store's compaction revision.
        compact_revision: Revision,
    },
}

/// A stream of watch events. When it ends without `Compacted`, resume from
/// the last revision seen + 1.
pub type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchEvent>> + Send>>;

/// The control plane's store: etcd in production, [`MemStore`] in tests.
#[async_trait::async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// The tier this store talks to.
    fn tier(&self) -> &Tier;
    /// Reads one key.
    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>>;
    /// Pages through a prefix; `opts.at` pins every page to one revision.
    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage>;
    /// Runs a transaction atomically.
    async fn txn(&self, txn: Txn) -> Result<TxnResult>;
    /// Puts, deletes, progress marks and `Compacted`, from revision `from`
    /// (0: from the next write).
    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream>;
    /// Grants a lease of `ttl`.
    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId>;
    /// One round trip; `None` means the lease no longer exists.
    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>>;
    /// Ends a lease and deletes its keys.
    async fn revoke_lease(&self, lease: LeaseId) -> Result<()>;
}
