//! The relay's `RemoteMaps`, served side: every controller answers reads of its own DC tier's `shardmaps/`
//! and `clusters/` for processes in other DCs (`sprig_meta::relay`'s
//! client), on `[controller] internal_addr`. Nothing else of the tier is
//! served, and nothing is written.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use sprig_meta::relay::{RelayGet, RelayKv, RelayList, RelayRequest, served};
use sprig_meta::{Consistency, Key, ListOpts, MetaStore};

fn reply(status: StatusCode, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap_or_default()
}

/// The served key, or a refusal (a response, so it is large).
#[allow(clippy::result_large_err)]
fn key_of(dc: &dyn MetaStore, body: &Bytes) -> std::result::Result<Key, Response> {
    let req: RelayRequest = serde_json::from_slice(body)
        .map_err(|e| reply(StatusCode::BAD_REQUEST, e.to_string().into_bytes()))?;
    let tier = dc.tier();
    if !served(tier, &req.key) {
        return Err(reply(
            StatusCode::FORBIDDEN,
            b"the relay serves only this DC's shardmaps/ and clusters/".to_vec(),
        ));
    }
    let rel = req
        .key
        .strip_prefix(tier.root().as_str())
        .unwrap_or_default();
    tier.key(rel)
        .map_err(|e| reply(StatusCode::BAD_REQUEST, e.to_string().into_bytes()))
}

async fn get(State(dc): State<Arc<dyn MetaStore>>, body: Bytes) -> Response {
    let key = match key_of(&*dc, &body) {
        Ok(k) => k,
        Err(r) => return r,
    };
    match dc.get(&key, Consistency::Linearizable).await {
        Ok(kv) => reply(
            StatusCode::OK,
            serde_json::to_vec(&RelayGet {
                kv: kv.as_ref().map(RelayKv::from_kv),
            })
            .unwrap_or_default(),
        ),
        Err(e) => reply(StatusCode::SERVICE_UNAVAILABLE, e.to_string().into_bytes()),
    }
}

async fn list(State(dc): State<Arc<dyn MetaStore>>, body: Bytes) -> Response {
    let prefix = match key_of(&*dc, &body) {
        Ok(k) => k,
        Err(r) => return r,
    };
    // Every page at the first page's revision.
    let mut kvs = Vec::new();
    let mut opts = ListOpts::default();
    let mut revision;
    loop {
        match dc.list(&prefix, opts.clone()).await {
            Ok(page) => {
                revision = page.revision;
                let last = page.kvs.last().map(|kv| kv.key.clone());
                kvs.extend(page.kvs.iter().map(RelayKv::from_kv));
                match (page.more, last) {
                    (true, Some(l)) => {
                        opts.at = Some(page.revision);
                        opts.after = Some(l);
                    }
                    _ => break,
                }
            }
            Err(e) => return reply(StatusCode::SERVICE_UNAVAILABLE, e.to_string().into_bytes()),
        }
    }
    reply(
        StatusCode::OK,
        serde_json::to_vec(&RelayList { kvs, revision }).unwrap_or_default(),
    )
}

/// The relay routes over this DC's tier.
pub fn router(dc: Arc<dyn MetaStore>) -> Router {
    Router::new()
        .route("/internal/v1/relay/get", post(get))
        .route("/internal/v1/relay/list", post(list))
        .with_state(dc)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use sprig_common::ids::DcId;
    use sprig_meta::relay::RoutedStore;
    use sprig_meta::{MemStore, Op, Tier, TierView, Txn, WatchEvent};

    use super::*;

    fn dc(s: &str) -> DcId {
        DcId::new(s).expect("dc")
    }

    async fn put(store: &dyn MetaStore, key: Key, v: &str) {
        store
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key,
                    value: v.as_bytes().to_vec(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .expect("put");
    }

    /// ams1's tier behind a real relay; fra1 reads it through a
    /// `RoutedStore`, as a gateway in fra1 does in two tiers.
    #[tokio::test]
    async fn another_dcs_maps_are_read_and_followed_through_the_relay() {
        let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let ams1: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("ams1"))));
        let fra1: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("fra1"))));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = l.local_addr().expect("addr");
        let app = router(Arc::clone(&ams1));
        tokio::spawn(async move { axum::serve(l, app).await });
        put(
            &*global,
            Tier::Global.key("dc-observe/ams1").expect("k"),
            &format!("{{\"v\":1,\"controllers\":[\"http://{addr}\"],\"writers\":[]}}"),
        )
        .await;
        let ams_tier = ams1.tier().clone();
        put(
            &*ams1,
            ams_tier.key("shardmaps/42/0").expect("k"),
            "{\"v\":1,\"role\":\"standby\"}",
        )
        .await;
        put(
            &*ams1,
            ams_tier.key("leases/writer/ams1-c1").expect("k"),
            "{}",
        )
        .await;
        let routed: Arc<dyn MetaStore> =
            Arc::new(RoutedStore::new(Arc::clone(&fra1), Arc::clone(&global)));
        let view = TierView::over(&routed, Tier::Dc(dc("ams1")));
        let key = view.tier().key("shardmaps/42/0").expect("k");
        let kv = view
            .get(&key, Consistency::Linearizable)
            .await
            .expect("get")
            .expect("relayed");
        assert!(String::from_utf8_lossy(&kv.value).contains("standby"));
        // Not served: other prefixes stay out of reach.
        let lease = view.tier().key("leases/writer/ams1-c1").expect("k");
        assert!(
            view.get(&lease, Consistency::Linearizable)
                .await
                .expect("get")
                .is_none(),
            "not routed: read locally, where it doesn't exist"
        );
        // A watch follows the promotion.
        let prefix = view.tier().key("shardmaps/").expect("k");
        let mut w = view.watch(&prefix, 0).await.expect("watch");
        let first = w.next().await.expect("event").expect("ok");
        assert!(matches!(first, WatchEvent::Put(_)));
        put(
            &*ams1,
            ams_tier.key("shardmaps/42/0").expect("k"),
            "{\"v\":1,\"role\":\"home\"}",
        )
        .await;
        let mut saw_home = false;
        for _ in 0..6 {
            if let Ok(Some(Ok(WatchEvent::Put(kv)))) =
                tokio::time::timeout(Duration::from_secs(3), w.next()).await
                && String::from_utf8_lossy(&kv.value).contains("home")
            {
                saw_home = true;
                break;
            }
        }
        assert!(saw_home, "the relayed watch saw the map turn home");
        // Writes never go through the relay.
        let own = fra1.tier().key("shardmaps/7/0").expect("k");
        put(&*routed, own.clone(), "{\"v\":1}").await;
        assert!(
            fra1.get(&own, Consistency::Linearizable)
                .await
                .expect("g")
                .is_some()
        );
    }
}
