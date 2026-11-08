//! etcd in a simulation: one [`MemStore`] reached over the simulated
//! network.
//!
//! [`SimEtcd`] owns the store and lives on a node of its own (`etcd` by
//! default). [`SimEtcd::client`] gives each simulated process a
//! [`MetaStore`] whose calls cross the network both ways: a partition or
//! loss on the way there fails the call after the request timeout without
//! effect, on the way back after the effect (an ambiguous commit, as with
//! a real etcd client). Watch streams end when the process loses the etcd
//! node, as a broken gRPC stream would. A background tick makes leases
//! expire on time even when nobody calls the store (`MemStore` expires them
//! lazily).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{
    Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MemStore, MetaStore, Revision, Tier,
    Txn, TxnResult, WatchEvent, WatchStream,
};
use tokio::sync::mpsc;
use tokio_stream_shim::ReceiverStream;

use super::net::SimNet;
use crate::faults::Node;

/// How often the lease tick runs.
pub const LEASE_TICK: Duration = Duration::from_millis(100);

/// Default per-call timeout of a simulated client.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// A simulated etcd: a [`MemStore`] on a network node.
#[derive(Debug, Clone)]
pub struct SimEtcd {
    node: Node,
    store: MemStore,
    net: SimNet,
    timeout: Duration,
}

impl SimEtcd {
    /// An etcd on `node`, with leases on `clock` (etcd's own clock), and a
    /// task that expires leases every [`LEASE_TICK`]. Call inside the
    /// simulation's runtime.
    #[must_use]
    pub fn new(node: Node, net: SimNet, clock: Arc<dyn Clock>) -> Self {
        let store = MemStore::with_clock(Tier::Global, clock);
        let tick = store.clone();
        let (tick_net, tick_node) = (net.clone(), node.clone());
        tokio::spawn(async move {
            let probe = Tier::Global.root();
            loop {
                tokio::time::sleep(LEASE_TICK).await;
                if tick_net.state(&tick_node) == super::net::NodeState::Running {
                    // Any call expires due leases; the result is irrelevant.
                    let _ = tick.get(&probe, Consistency::Serializable).await;
                }
            }
        });
        Self {
            node,
            store,
            net,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Sets the per-call timeout of clients made afterwards.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The node etcd runs on.
    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// The store itself, for checks that bypass the network.
    #[must_use]
    pub fn store(&self) -> &MemStore {
        &self.store
    }

    /// A client for the process on `from`, talking to `tier`.
    #[must_use]
    pub fn client(&self, from: &Node, tier: Tier) -> Arc<dyn MetaStore> {
        Arc::new(SimMeta {
            from: from.clone(),
            etcd: self.clone(),
            store: self.store.with_tier(tier),
        })
    }
}

/// One process's view of a [`SimEtcd`].
#[derive(Debug)]
struct SimMeta {
    from: Node,
    etcd: SimEtcd,
    store: MemStore,
}

fn unavailable(what: &str, from: &Node, why: &str) -> SprigError {
    SprigError::new(
        SprigCode::MetaUnavailable,
        format!("etcd {what} from {from}: {why} (simulated)"),
    )
}

impl SimMeta {
    async fn leg(&self, there: bool, what: &str) -> Result<()> {
        let (a, b) = if there {
            (&self.from, &self.etcd.node)
        } else {
            (&self.etcd.node, &self.from)
        };
        let started = tokio::time::Instant::now();
        match self.etcd.net.transit(a, b).await {
            Ok(()) if self.etcd.net.state(&self.etcd.node) == super::net::NodeState::Running => {
                Ok(())
            }
            r => {
                // A client sees a timeout, not an instant error.
                let left = self.etcd.timeout.saturating_sub(started.elapsed());
                tokio::time::sleep(left).await;
                let why = match r {
                    Err(e) => e.to_string(),
                    Ok(()) => "etcd paused".to_owned(),
                };
                let leg = if there { "request" } else { "response" };
                self.etcd
                    .net
                    .trace()
                    .record(format!("etcd {what} from {} {leg} {why}", self.from));
                Err(unavailable(what, &self.from, &format!("{leg} {why}")))
            }
        }
    }

    async fn call<T, F, Fut>(&self, what: &str, f: F) -> Result<T>
    where
        F: FnOnce(MemStore) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.leg(true, what).await?;
        let r = f(self.store.clone()).await;
        self.etcd.net.trace().record(format!(
            "etcd {what} from {} {}",
            self.from,
            if r.is_ok() { "ok" } else { "err" }
        ));
        self.leg(false, what).await?;
        r
    }
}

#[async_trait::async_trait]
impl MetaStore for SimMeta {
    fn tier(&self) -> &Tier {
        self.store.tier()
    }

    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
        let key = key.clone();
        self.call("get", |s| async move { s.get(&key, c).await })
            .await
    }

    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        let prefix = prefix.clone();
        self.call("list", |s| async move { s.list(&prefix, opts).await })
            .await
    }

    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        self.call("txn", |s| async move { s.txn(txn).await }).await
    }

    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        let p = prefix.clone();
        let mut inner = self
            .call("watch", |s| async move { s.watch(&p, from).await })
            .await?;
        let (tx, rx) = mpsc::channel::<Result<WatchEvent>>(sprig_meta::mem::WATCH_QUEUE);
        let net = self.etcd.net.clone();
        let (me, etcd) = (self.from.clone(), self.etcd.node.clone());
        let mut changes = net.changes();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => return,
                    r = changes.changed() => {
                        if r.is_err() || !net.reachable(&me, &etcd) {
                            net.trace().record(format!("etcd watch of {me} broken"));
                            let _ = tx.send(Err(unavailable("watch", &me, "stream broken"))).await;
                            return;
                        }
                    }
                    ev = inner.next() => {
                        let Some(ev) = ev else { return };
                        if net.transit(&etcd, &me).await.is_err() {
                            net.trace().record(format!("etcd watch of {me} broken"));
                            let _ = tx.send(Err(unavailable("watch", &me, "stream broken"))).await;
                            return;
                        }
                        if tx.send(ev).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        self.call("grant", |s| async move { s.grant_lease(ttl).await })
            .await
    }

    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        self.call("keep_alive", |s| async move { s.keep_alive(lease).await })
            .await
    }

    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        self.call("revoke", |s| async move { s.revoke_lease(lease).await })
            .await
    }
}

/// A `Stream` over an mpsc receiver (tokio-stream's wrapper, without the
/// dependency).
mod tokio_stream_shim {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::sync::mpsc;

    pub(super) struct ReceiverStream<T>(mpsc::Receiver<T>);

    impl<T> ReceiverStream<T> {
        pub(super) fn new(rx: mpsc::Receiver<T>) -> Self {
            Self(rx)
        }
    }

    impl<T> futures::Stream for ReceiverStream<T> {
        type Item = T;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            self.0.poll_recv(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{SimClock, SimRng, Trace};
    use sprig_meta::{Cmp, Op};

    fn setup() -> (SimNet, SimEtcd) {
        let net = SimNet::new(SimRng::new(5), Trace::new());
        let etcd = SimEtcd::new(Node::from("etcd"), net.clone(), Arc::new(SimClock::new()));
        (net, etcd)
    }

    fn put(key: &Key) -> Txn {
        Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key: key.clone(),
                value: b"v".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        }
    }

    #[tokio::test(start_paused = true)]
    async fn calls_cross_the_network() {
        let (net, etcd) = setup();
        let a = Node::from("a");
        let c = etcd.client(&a, Tier::Global);
        let key = Tier::Global.key("x").unwrap();
        assert!(c.txn(put(&key)).await.unwrap().succeeded);
        let id = net.partition(std::slice::from_ref(&a), &[Node::from("etcd")]);
        let t0 = tokio::time::Instant::now();
        let e = c.get(&key, Consistency::Linearizable).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::MetaUnavailable));
        assert!(e.retryable);
        assert_eq!(t0.elapsed(), DEFAULT_TIMEOUT);
        net.heal(id);
        assert!(
            c.get(&key, Consistency::Linearizable)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn leases_expire_without_calls_and_watches_break() {
        let (net, etcd) = setup();
        let t0 = tokio::time::Instant::now();
        let a = Node::from("a");
        let c = etcd.client(&a, Tier::Global);
        let key = Tier::Global.key("leader").unwrap();
        let lease = c.grant_lease(Duration::from_secs(3)).await.unwrap();
        c.txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key.clone(),
                value: b"me".to_vec(),
                lease: Some(lease),
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let b = Node::from("b");
        let mut w = etcd.client(&b, Tier::Global).watch(&key, 0).await.unwrap();
        // Nobody calls the store; the tick still deletes the key on time.
        let ev = w.next().await.unwrap().unwrap();
        assert!(matches!(ev, WatchEvent::Delete { .. }), "{ev:?}");
        let at = t0.elapsed();
        assert!(
            at >= Duration::from_secs(3) && at < Duration::from_millis(3200),
            "{at:?}"
        );

        let mut w = etcd.client(&b, Tier::Global).watch(&key, 0).await.unwrap();
        net.partition(&[b], &[Node::from("etcd")]);
        assert!(w.next().await.unwrap().is_err());
    }
}
