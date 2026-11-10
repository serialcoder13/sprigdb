//! The hubs over an in-memory transport: three gateways in one process.

use std::sync::RwLock;

use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::pubsub::{Kind, Msg};

#[derive(Default)]
struct Net {
    nodes: RwLock<BTreeMap<String, (Arc<Mesh>, bool)>>,
    /// Still listed as members, but unreachable (a killed gateway whose
    /// registration hasn't expired).
    dead: RwLock<HashSet<String>>,
}

impl Net {
    fn get(&self, to: &str) -> Result<Arc<Mesh>, String> {
        if self.dead.read().unwrap().contains(to) {
            return Err(format!("{to} unreachable"));
        }
        match self.nodes.read().unwrap().get(to) {
            Some((m, true)) => Ok(Arc::clone(m)),
            _ => Err(format!("{to} unreachable")),
        }
    }
}

struct Mem {
    me: String,
    net: Arc<Net>,
}

#[async_trait]
impl Transport for Mem {
    fn me(&self) -> String {
        self.me.clone()
    }
    fn members(&self) -> Vec<String> {
        self.net
            .nodes
            .read()
            .unwrap()
            .iter()
            .filter(|(_, (_, up))| *up)
            .map(|(k, _)| k.clone())
            .collect()
    }
    async fn interest(&self, to: &str, m: InterestMsg) -> Result<(), String> {
        self.net.get(to)?.on_interest(&m);
        Ok(())
    }
    async fn publish(&self, to: &str, m: PublishMsg) -> Result<i64, String> {
        Ok(self.net.get(to)?.on_publish(m).await)
    }
    async fn deliver(&self, to: &str, m: PublishMsg) -> Result<i64, String> {
        Ok(self.net.get(to)?.on_deliver(&m))
    }
    async fn wake(&self, to: &str, m: WakeMsg) -> Result<bool, String> {
        Ok(self.net.get(to)?.on_wake(m).await)
    }
    async fn introspect(
        &self,
        to: &str,
        scope: &str,
        shard: bool,
    ) -> Result<Introspection, String> {
        Ok(self.net.get(to)?.on_introspect(scope, shard))
    }
}

fn gateway(net: &Arc<Net>, id: &str) -> (Arc<Env>, Arc<Mesh>) {
    let env = Arc::new(Env::new(crate::Config::default(), id));
    let mesh = Mesh::new(
        Arc::new(Mem {
            me: id.to_owned(),
            net: Arc::clone(net),
        }),
        MeshOpts {
            refresh: Duration::from_millis(200),
            ttl: Duration::from_millis(1000),
            ..MeshOpts::default()
        },
    );
    mesh.attach(&env);
    net.nodes
        .write()
        .unwrap()
        .insert(id.to_owned(), (Arc::clone(&mesh), true));
    (env, mesh)
}

#[test]
fn rendezvous_is_stable_and_spreads() {
    let m: Vec<String> = (0..4).map(|i| format!("gw-{i}")).collect();
    let mut hits = BTreeMap::new();
    for t in 0..400 {
        let k = format!("t{t}/c");
        let a = rendezvous(&k, &m).unwrap();
        // Removing another member never moves the key.
        let other: Vec<String> = m.iter().filter(|x| *x != a).skip(1).cloned().collect();
        let mut fewer = m.clone();
        fewer.retain(|x| !other.contains(x) || x == a);
        assert_eq!(rendezvous(&k, &fewer), Some(a));
        *hits.entry(a.to_owned()).or_insert(0) += 1;
    }
    assert_eq!(hits.len(), 4);
    assert!(hits.values().all(|n| *n > 50), "{hits:?}");
    assert_eq!(rendezvous("x", &[]), None);
}

#[test]
fn hub_table_targets_patterns_and_expiry() {
    let mut t = HubTable::default();
    let now = Instant::now();
    let ttl = Duration::from_secs(10);
    let m = |from: &str, add: Vec<Interest>| InterestMsg {
        scope: "t1".into(),
        hub_key: "c".into(),
        from: from.into(),
        full: false,
        add,
        remove: Vec::new(),
    };
    t.apply(
        &m("a", vec![Interest::Channel(Bytes::from("news"))]),
        now,
        ttl,
    );
    t.apply(
        &m("b", vec![Interest::Pattern(Bytes::from("n*"))]),
        now,
        ttl,
    );
    t.apply(
        &m("c", vec![Interest::Channel(Bytes::from("other"))]),
        now,
        ttl,
    );
    let news = Bytes::from("news");
    assert_eq!(t.targets("t1", "c", &news, false, now, ttl), vec!["a", "b"]);
    assert!(t.targets("t2", "c", &news, false, now, ttl).is_empty());
    // b's entry expires; a refreshes.
    let later = now + Duration::from_secs(11);
    t.apply(
        &InterestMsg {
            full: true,
            ..m("a", vec![Interest::Channel(news.clone())])
        },
        later,
        ttl,
    );
    assert_eq!(t.targets("t1", "c", &news, false, later, ttl), vec!["a"]);
    // The first waiter of a tenant is reported once.
    assert!(t.apply(
        &m("c", vec![Interest::Wait(0, Bytes::from("q"))]),
        later,
        ttl
    ));
    assert!(!t.apply(
        &m("a", vec![Interest::Wait(0, Bytes::from("q"))]),
        later,
        ttl
    ));
    let (w, any) = t.waiters("t1", 0, &[Bytes::from("q")], later, ttl);
    assert_eq!(w, vec!["a", "c"]);
    assert!(any);
}

fn sub(
    env: &Env,
    scope: &str,
    kind: Kind,
    name: &str,
    id: u64,
) -> tokio::sync::mpsc::UnboundedReceiver<Msg> {
    let (tx, rx) = unbounded_channel();
    env.hub
        .subscribe(scope, kind, &Bytes::from(name.to_owned()), id, tx);
    rx
}

#[tokio::test]
async fn publish_reaches_every_gateway() {
    let net = Arc::new(Net::default());
    let (e1, m1) = gateway(&net, "gw-1");
    let (e2, m2) = gateway(&net, "gw-2");
    let (e3, m3) = gateway(&net, "gw-3");
    let mut r1 = sub(&e1, "t", Kind::Channel, "news", 1);
    m1.hold("t", Interest::Channel(Bytes::from("news")))
        .await
        .unwrap();
    let mut r2 = sub(&e2, "t", Kind::Pattern, "n*", 2);
    m2.hold("t", Interest::Pattern(Bytes::from("n*")))
        .await
        .unwrap();
    let mut r2b = sub(&e2, "t", Kind::Channel, "news", 3);
    m2.hold("t", Interest::Channel(Bytes::from("news")))
        .await
        .unwrap();
    // Another tenant's subscriber on gw-3 gets nothing.
    let mut r3 = sub(&e3, "u", Kind::Channel, "news", 4);
    m3.hold("u", Interest::Channel(Bytes::from("news")))
        .await
        .unwrap();
    let n = m3
        .publish(PublishMsg {
            scope: "t".into(),
            channel: Bytes::from("news"),
            data: Bytes::from("hi"),
            shard: false,
        })
        .await
        .unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        r1.try_recv().unwrap(),
        Msg::Message("news".into(), "hi".into())
    );
    assert_eq!(
        r2.try_recv().unwrap(),
        Msg::PMessage("n*".into(), "news".into(), "hi".into())
    );
    assert_eq!(
        r2b.try_recv().unwrap(),
        Msg::Message("news".into(), "hi".into())
    );
    assert!(r3.try_recv().is_err());
    // PUBSUB across gateways.
    let i = m1.introspect("t", false).await;
    assert_eq!(i.channels.get(&Bytes::from("news")), Some(&2));
    assert_eq!(i.patterns.len(), 1);
    // Sharded channels go to their own hubs.
    let mut s1 = sub(&e1, "t", Kind::Shard, "orders", 5);
    m1.hold("t", Interest::Shard(Bytes::from("orders")))
        .await
        .unwrap();
    let n = m2
        .publish(PublishMsg {
            scope: "t".into(),
            channel: Bytes::from("orders"),
            data: Bytes::from("o1"),
            shard: true,
        })
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        s1.try_recv().unwrap(),
        Msg::SMessage("orders".into(), "o1".into())
    );
    // Unsubscribing tells the hub.
    e1.hub
        .unsubscribe("t", Kind::Channel, &Bytes::from("news"), 1);
    m1.release("t", Interest::Channel(Bytes::from("news")));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let n = m3
        .publish(PublishMsg {
            scope: "t".into(),
            channel: Bytes::from("news"),
            data: Bytes::from("2"),
            shard: false,
        })
        .await
        .unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn a_new_hub_fills_from_refreshes() {
    let net = Arc::new(Net::default());
    let gws: BTreeMap<String, (Arc<Env>, Arc<Mesh>)> = ["gw-1", "gw-2", "gw-3"]
        .iter()
        .map(|g| ((*g).to_owned(), gateway(&net, g)))
        .collect();
    let members: Vec<String> = gws.keys().cloned().collect();
    let hub = rendezvous("t/c", &members).unwrap().to_owned();
    let others: Vec<&String> = members.iter().filter(|g| **g != hub).collect();
    let (se, sm) = &gws[others[0]];
    let (_, pm) = &gws[others[1]];
    let mut r = sub(se, "t", Kind::Channel, "c", 1);
    sm.hold("t", Interest::Channel(Bytes::from("c")))
        .await
        .unwrap();
    // Take the hub away: the publisher's view changes at once, the
    // subscriber's hub at its next refresh.
    net.nodes.write().unwrap().get_mut(&hub).unwrap().1 = false;
    let mut got = false;
    for _ in 0..40 {
        sm.refresh(false).await;
        let n = pm
            .publish(PublishMsg {
                scope: "t".into(),
                channel: Bytes::from("c"),
                data: Bytes::from("x"),
                shard: false,
            })
            .await
            .unwrap_or(0);
        if n == 1 {
            got = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(got, "the new hub never learned the subscription");
    assert!(r.try_recv().is_ok());
}

#[tokio::test]
async fn wakes_cross_gateways_and_no_waiter_answers_are_cached() {
    let net = Arc::new(Net::default());
    let (e1, m1) = gateway(&net, "gw-1");
    let (_e2, m2) = gateway(&net, "gw-2");
    let key = Bytes::from("q");
    let note = e1.waits.notify("t", 0, &key);
    let w = note.notified();
    tokio::pin!(w);
    w.as_mut().enable();
    // No waiter yet: the hub says so, and gw-2 stops asking.
    m2.wake("t", 0, vec![key.clone()]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(lock(&m2.no_waiters).contains_key("t"));
    // A waiter registers: gw-2 hears about it and asks again.
    m1.hold("t", Interest::Wait(0, key.clone())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!lock(&m2.no_waiters).contains_key("t"));
    m2.wake("t", 0, vec![key.clone()]);
    tokio::time::timeout(Duration::from_secs(2), w)
        .await
        .expect("woken across gateways");
}

#[tokio::test]
async fn a_dead_hub_still_listed_is_passed_over() {
    let net = Arc::new(Net::default());
    let gws: BTreeMap<String, (Arc<Env>, Arc<Mesh>)> = ["gw-1", "gw-2", "gw-3"]
        .iter()
        .map(|g| ((*g).to_owned(), gateway(&net, g)))
        .collect();
    let members: Vec<String> = gws.keys().cloned().collect();
    let hub = rendezvous("t/c", &members).unwrap().to_owned();
    let others: Vec<&String> = members.iter().filter(|g| **g != hub).collect();
    let (se, sm) = &gws[others[0]];
    let (_, pm) = &gws[others[1]];
    let mut r = sub(se, "t", Kind::Channel, "c", 1);
    sm.hold("t", Interest::Channel(Bytes::from("c")))
        .await
        .unwrap();
    net.dead.write().unwrap().insert(hub.clone());
    let mut got = false;
    for _ in 0..10 {
        // The periodic forced refresh finds the hub dead, then the next
        // one syncs with the new hub.
        sm.refresh(true).await;
        sm.refresh(false).await;
        let n = pm
            .publish(PublishMsg {
                scope: "t".into(),
                channel: Bytes::from("c"),
                data: Bytes::from("x"),
                shard: false,
            })
            .await
            .unwrap_or(0);
        if n == 1 {
            got = true;
            break;
        }
    }
    assert!(got, "publishes never moved off the dead hub");
    assert!(r.try_recv().is_ok());
}
