//! Leader election with a fencing token.
//!
//! ```text
//! campaign(key, me):
//! 1. session = Session::open(ttl)            # control.leader_lease_seconds
//! 2. loop:
//!    a. r = txn(when [Missing(key)], then [Put(key, me, session.lease)], else [Get(key)])
//!    b. if r.succeeded: return Leadership { token = r.revision, session }
//!    c. watch key from r.gets[0].mod_rev + 1; wait for Delete or session loss
//! guarded(l, ops) = Txn { when [CreateRev(key, l.token)], then ops }
//! on session loss, or a guarded txn failing its compare: fire step-down
//! ```
//!
//! The token is the revision that created the leader key, so a deposed
//! leader's guarded writes cannot commit.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::session::{Session, SessionOpts, lost_error};
use crate::{
    Cmp, Consistency, Key, KeyValue, ListOpts, MetaStore, Op, OpResult, Record, Revision, Txn,
    TxnResult, Versioned, WatchEvent,
};

/// Wait before retrying after a store error while campaigning or observing.
const RETRY_AFTER: Duration = Duration::from_millis(500);

/// The value of a `leader` key: who leads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// The leading process (its node id).
    pub holder: String,
    /// Its data center; set for the global leader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc: Option<String>,
    /// Its build version.
    pub build: String,
}

impl Versioned for LeaderRecord {
    const V: u32 = 1;
}

/// A leader as observers see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leader {
    /// Who leads.
    pub record: LeaderRecord,
    /// Its fencing token: the leader key's create revision.
    pub token: Revision,
}

impl Leader {
    fn from_kv(kv: &KeyValue) -> Result<Self> {
        Ok(Self {
            record: Record::<LeaderRecord>::decode(&kv.value)?.body,
            token: kv.create_revision,
        })
    }
}

/// Held leadership: a fencing token and the session that keeps it.
#[derive(Debug, Clone)]
pub struct Leadership {
    /// The revision that created the leader key.
    pub token: Revision,
    /// The session the leader key is bound to.
    pub session: Session,
    key: Key,
    deposed: Arc<watch::Sender<bool>>,
}

impl Leadership {
    /// The leader key.
    #[must_use]
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Whether this process may still act as leader: the session is alive
    /// and no guarded transaction has failed its compare.
    #[must_use]
    pub fn is_leading(&self) -> bool {
        !*self.deposed.borrow() && !self.session.is_lost()
    }

    /// Resolves when leadership ends: the session is lost, a guarded
    /// transaction failed its compare, or the leader resigned. Every loop
    /// stops when it fires.
    pub async fn stepped_down(&self) {
        let mut rx = self.deposed.subscribe();
        tokio::select! {
            () = self.session.lost() => {}
            _ = rx.wait_for(|d| *d) => {}
        }
    }

    /// A transaction that commits only while this leadership holds.
    #[must_use]
    pub fn guarded(&self, then: Vec<Op>) -> Txn {
        Txn {
            when: vec![Cmp::CreateRev(self.key.clone(), self.token)],
            then,
            otherwise: Vec::new(),
        }
    }

    /// Runs `then` guarded by the token. Fails without writing if the
    /// session is lost or the token no longer holds; a failed compare also
    /// fires the step-down.
    ///
    /// # Errors
    /// 39504 `ER_SPRIG_NOT_LEADER`, not retryable, when this process no
    /// longer leads; the store's error otherwise.
    pub async fn commit(&self, then: Vec<Op>) -> Result<TxnResult> {
        if !self.is_leading() {
            return Err(not_leader());
        }
        let r = self.session.store().txn(self.guarded(then)).await?;
        if !r.succeeded {
            tracing::warn!(key = %self.key, token = self.token, "guarded txn failed its compare; stepping down");
            self.deposed.send_replace(true);
            return Err(not_leader());
        }
        Ok(r)
    }

    /// Runs `then` if this leadership holds and every compare in `when`
    /// holds: a guarded write that also checks the records it read (for
    /// example `ModRev` of a setting record). Unlike [`commit`](Self::commit),
    /// a failed `when` compare is not a lost leadership: the result comes
    /// back with `succeeded == false` and the caller re-reads and retries.
    /// Only a token that no longer holds fires the step-down.
    ///
    /// # Errors
    /// As [`commit`](Self::commit).
    pub async fn commit_if(&self, when: Vec<Cmp>, then: Vec<Op>) -> Result<TxnResult> {
        if !self.is_leading() {
            return Err(not_leader());
        }
        let mut txn = self.guarded(then);
        txn.when.extend(when);
        txn.otherwise = vec![Op::Get(self.key.clone())];
        let r = self.session.store().txn(txn).await?;
        if r.succeeded {
            return Ok(r);
        }
        let still_ours = matches!(
            r.results.first(),
            Some(OpResult::Get(Some(kv))) if kv.create_revision == self.token
        );
        if !still_ours {
            tracing::warn!(key = %self.key, token = self.token, "guarded txn failed its compare; stepping down");
            self.deposed.send_replace(true);
            return Err(not_leader());
        }
        Ok(r)
    }

    /// Gives leadership up: deletes the leader key if it is still ours and
    /// revokes the session.
    ///
    /// # Errors
    /// The store's error; the lease then ends on its TTL.
    pub async fn resign(&self) -> Result<()> {
        self.deposed.send_replace(true);
        let txn = Txn {
            when: vec![Cmp::CreateRev(self.key.clone(), self.token)],
            then: vec![Op::Delete(self.key.clone())],
            otherwise: Vec::new(),
        };
        let deleted = self.session.store().txn(txn).await;
        let closed = self.session.close().await;
        deleted.and(closed)
    }
}

/// The error for work refused because this process no longer leads.
/// Not retryable: the caller must step down, not retry.
fn not_leader() -> SprigError {
    SprigError::new(SprigCode::NotLeader, "leadership lost")
}

/// Campaigns for leader keys.
#[async_trait::async_trait]
pub trait Elector: Send + Sync {
    /// Waits until this process leads at `key`.
    async fn campaign(&self, key: &Key, me: &LeaderRecord) -> Result<Leadership>;
    /// A txn that commits only while `l` still leads.
    fn guarded(&self, l: &Leadership, then: Vec<Op>) -> Txn;
}

/// The [`Elector`] over a [`MetaStore`].
#[derive(Clone)]
pub struct Election {
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    opts: SessionOpts,
}

impl std::fmt::Debug for Election {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Election")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl Election {
    /// An elector whose sessions use `opts` (read at each campaign, so a new
    /// `control.leader_lease_seconds` applies at the next one).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, clock: Arc<dyn Clock>, opts: SessionOpts) -> Self {
        Self { store, clock, opts }
    }

    async fn contend(&self, session: &Session, key: &Key, value: &[u8]) -> Result<Leadership> {
        let lease = session.lease();
        loop {
            if session.is_lost() {
                return Err(lost_error("session"));
            }
            let txn = Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: value.to_vec(),
                    lease: Some(lease),
                }],
                otherwise: vec![Op::Get(key.clone())],
            };
            let r = match self.store.txn(txn).await {
                Ok(r) => r,
                Err(e) if e.retryable => {
                    tracing::warn!(%key, error = %e, "campaign txn failed; retrying");
                    tokio::time::sleep(RETRY_AFTER).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if r.succeeded {
                return Ok(Self::leadership(key, r.revision, session));
            }
            let Some(OpResult::Get(Some(current))) = r.results.first() else {
                continue; // deleted in between: try again
            };
            // An earlier attempt committed but its reply was lost.
            if current.lease == Some(lease) {
                return Ok(Self::leadership(key, current.create_revision, session));
            }
            let mut w = match self.store.watch(key, current.mod_revision + 1).await {
                Ok(w) => w,
                Err(e) if e.retryable => {
                    tokio::time::sleep(RETRY_AFTER).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            loop {
                tokio::select! {
                    () = session.lost() => return Err(lost_error("session")),
                    ev = w.next() => match ev {
                        Some(Ok(WatchEvent::Delete { key: k, .. })) if &k == key => break,
                        Some(Ok(WatchEvent::Compacted { .. }) | Err(_)) | None => break,
                        Some(Ok(_)) => {}
                    },
                }
            }
        }
    }

    fn leadership(key: &Key, token: Revision, session: &Session) -> Leadership {
        tracing::info!(%key, token, "won leadership");
        Leadership {
            token,
            session: session.clone(),
            key: key.clone(),
            deposed: Arc::new(watch::channel(false).0),
        }
    }
}

#[async_trait::async_trait]
impl Elector for Election {
    async fn campaign(&self, key: &Key, me: &LeaderRecord) -> Result<Leadership> {
        let value = Record::new(me.clone()).encode()?;
        let session =
            Session::open(Arc::clone(&self.store), Arc::clone(&self.clock), self.opts).await?;
        match self.contend(&session, key, &value).await {
            Ok(l) => Ok(l),
            Err(e) => {
                // Best effort: the lease ends on its TTL otherwise.
                let _ = session.close().await;
                Err(e)
            }
        }
    }

    fn guarded(&self, l: &Leadership, then: Vec<Op>) -> Txn {
        l.guarded(then)
    }
}

/// Changes of the leader at a key: `None` while nobody leads.
pub type ObserveStream = Pin<Box<dyn Stream<Item = Option<Leader>> + Send>>;

/// Follows the leader at `key`. The first item is the current state; then
/// one item per change. Store errors are retried; the stream never ends
/// while it is polled. Dropping it stops the background task.
#[must_use]
pub fn observe(store: Arc<dyn MetaStore>, key: Key) -> ObserveStream {
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        let mut last = Seen::Nothing;
        loop {
            tokio::select! {
                () = tx.closed() => return,
                r = follow(&*store, &key, &tx, &mut last) => {
                    if r.is_err() {
                        // Receiver gone.
                        return;
                    }
                }
            }
            tokio::time::sleep(RETRY_AFTER).await;
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

/// What an observer last sent.
enum Seen {
    /// Nothing yet.
    Nothing,
    /// This leader, or `None` for no leader.
    Leader(Option<Leader>),
}

/// Reads the leader, then follows its changes until the watch ends.
/// `Err` only when the receiver is gone.
async fn follow(
    store: &dyn MetaStore,
    key: &Key,
    tx: &mpsc::Sender<Option<Leader>>,
    last: &mut Seen,
) -> std::result::Result<(), ()> {
    async fn emit(
        tx: &mpsc::Sender<Option<Leader>>,
        last: &mut Seen,
        now: Option<Leader>,
    ) -> std::result::Result<(), ()> {
        if !matches!(last, Seen::Leader(l) if *l == now) {
            *last = Seen::Leader(now.clone());
            tx.send(now).await.map_err(|_| ())?;
        }
        Ok(())
    }

    // A listing gives the revision of the read even when the key is missing.
    let page = match store
        .list(
            key,
            ListOpts {
                limit: 1,
                ..ListOpts::default()
            },
        )
        .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::debug!(%key, error = %e, "observe read failed");
            return Ok(());
        }
    };
    let current = page.kvs.first().filter(|kv| &kv.key == key);
    match current.map(Leader::from_kv).transpose() {
        Ok(l) => emit(tx, last, l).await?,
        Err(e) => tracing::warn!(%key, error = %e, "bad leader record"),
    }
    let Ok(mut w) = store.watch(key, page.revision + 1).await else {
        return Ok(());
    };
    while let Some(ev) = w.next().await {
        match ev {
            Ok(WatchEvent::Put(kv)) if &kv.key == key => match Leader::from_kv(&kv) {
                Ok(l) => emit(tx, last, Some(l)).await?,
                Err(e) => tracing::warn!(%key, error = %e, "bad leader record"),
            },
            Ok(WatchEvent::Delete { key: k, .. }) if &k == key => emit(tx, last, None).await?,
            Ok(WatchEvent::Compacted { .. }) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
    Ok(())
}

/// Reads the current leader at `key`, if any.
///
/// # Errors
/// The store's error, or internal for a bad record.
pub async fn current_leader(store: &dyn MetaStore, key: &Key) -> Result<Option<Leader>> {
    store
        .get(key, Consistency::Linearizable)
        .await?
        .map(|kv| Leader::from_kv(&kv))
        .transpose()
}
