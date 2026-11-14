//! Retryable writes under a lost reply: cut a connection
//! after `COMMIT` but before the reply. The driver's retry must leave
//! exactly one copy, with identical result counts.
//!
//! A small proxy sits between the Rust driver (`retryWrites` on, its
//! default) and the gateway. Armed with a command and a collection, it
//! forwards the next such command, reads the gateway's reply, and closes
//! the client connection instead of passing the reply on, so the write
//! committed but the driver saw a network error. The driver retries on a
//! new connection with the same `lsid` and `txnNumber`; the gateway must
//! answer from its record.
//!
//! `SPRIG_TEST_MONGO_URI` names the gateway (as in `driver_live.rs`); the
//! tests are skipped without it. They use the databases `r5mongo2_retry`
//! and `r5mongo3_retry_cu` and drop them at the end.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_wrap
)]

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{ClientOptions, ReturnDocument};
use mongodb::{Client, Collection};
use sprig_proto_mongo::codec;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

/// The proxy's shared state: what to cut, and how many replies it cut.
#[derive(Default)]
struct Faults {
    armed: Mutex<Option<(String, String)>>,
    fired: AtomicU32,
}

impl Faults {
    fn arm(&self, cmd: &str, coll: &str) {
        *self.armed.lock().unwrap() = Some((cmd.to_owned(), coll.to_owned()));
    }

    /// Whether `msg` is the armed command (disarming it).
    fn matches(&self, msg: &[u8]) -> bool {
        let mut g = self.armed.lock().unwrap();
        let Some((cmd, coll)) = g.as_ref() else {
            return false;
        };
        let Ok((_, codec::Frame::Msg(m), _)) = codec::decode(msg) else {
            return false;
        };
        let Ok(body) = m.body.to_document() else {
            return false;
        };
        let hit = body.keys().next().map(String::as_str) == Some(cmd.as_str())
            && body.get_str(cmd.as_str()).ok() == Some(coll.as_str());
        if hit {
            *g = None;
        }
        hit
    }
}

async fn proxy(upstream: String, faults: Arc<Faults>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = l.accept().await else {
                return;
            };
            let up = upstream.clone();
            let f = Arc::clone(&faults);
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(&up).await else {
                    return;
                };
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let cut = Arc::new(AtomicBool::new(false));
                let cut2 = Arc::clone(&cut);
                let f2 = Arc::clone(&f);
                let c2s = tokio::spawn(async move {
                    while let Ok(Some(m)) = codec::read_message(&mut cr).await {
                        if f2.matches(&m) {
                            cut2.store(true, Ordering::SeqCst);
                        }
                        if sw.write_all(&m).await.is_err() {
                            return;
                        }
                    }
                });
                while let Ok(Some(m)) = codec::read_message(&mut sr).await {
                    if cut.swap(false, Ordering::SeqCst) {
                        // The write committed; the driver never hears.
                        f.fired.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    if cw.write_all(&m).await.is_err() {
                        break;
                    }
                }
                c2s.abort();
                let _ = cw.shutdown().await;
            });
        }
    });
    port
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        mongodb::error::ErrorKind::InsertMany(i) => i
            .write_errors
            .as_ref()
            .and_then(|w| w.first())
            .map_or(-1, |w| w.code),
        _ => -1,
    }
}

async fn ids(c: &Collection<Document>, f: Document) -> Vec<Bson> {
    let v: Vec<Document> = c
        .find(f)
        .sort(doc! {"_id": 1})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    v.into_iter()
        .map(|d| d.get("_id").cloned().unwrap())
        .collect()
}

#[tokio::test]
async fn retried_writes_run_once() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let target = ClientOptions::parse(&u).await.unwrap();
    let host = target.hosts[0].to_string();
    let faults = Arc::new(Faults::default());
    let port = proxy(host.clone(), Arc::clone(&faults)).await;
    let mut o = ClientOptions::parse(u.replacen(&host, &format!("127.0.0.1:{port}"), 1))
        .await
        .unwrap();
    assert_ne!(o.retry_writes, Some(false));
    o.app_name = Some("r5mongo2-retry~dc-a".into());
    let client = Client::with_options(o).unwrap();
    let db = client.database("r5mongo2_retry");
    let _ = db.drop().await;
    let c = db.collection::<Document>("rw");
    let mut cuts = 0;
    let mut check = |what: &str| {
        cuts += 1;
        assert_eq!(
            faults.fired.load(Ordering::SeqCst),
            cuts,
            "{what}: the proxy didn't cut a reply"
        );
        eprintln!("{what}: reply cut, retried, answered once");
    };

    // insertOne: one copy, and the retry answers ok (not 11000).
    c.insert_one(doc! {"_id": 0, "v": 0}).await.unwrap();
    faults.arm("insert", "rw");
    c.insert_one(doc! {"_id": 1, "v": 1}).await.unwrap();
    check("insertOne");
    assert_eq!(c.count_documents(doc! {"_id": 1}).await.unwrap(), 1);

    // updateOne $inc: applied once, counts of the first attempt.
    faults.arm("update", "rw");
    let r = c
        .update_one(doc! {"_id": 1}, doc! {"$inc": {"v": 1}})
        .await
        .unwrap();
    check("updateOne $inc");
    assert_eq!((r.matched_count, r.modified_count), (1, 1));
    let d = c.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("v").unwrap(), 2, "{d}");

    // findOneAndUpdate: applied once; the stored image comes back.
    faults.arm("findAndModify", "rw");
    let d = c
        .find_one_and_update(doc! {"_id": 1}, doc! {"$inc": {"v": 10}})
        .return_document(ReturnDocument::After)
        .await
        .unwrap()
        .unwrap();
    check("findOneAndUpdate");
    assert_eq!(d.get_i32("v").unwrap(), 12, "{d}");
    let d = c.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("v").unwrap(), 12, "{d}");

    // findOneAndDelete: the deleted image, not null.
    c.insert_one(doc! {"_id": 2, "v": 2}).await.unwrap();
    faults.arm("findAndModify", "rw");
    let d = c.find_one_and_delete(doc! {"_id": 2}).await.unwrap();
    check("findOneAndDelete");
    assert_eq!(d.unwrap().get_i32("v").unwrap(), 2);

    // deleteOne: reports the delete it did (a rerun would report 0).
    faults.arm("delete", "rw");
    let r = c.delete_one(doc! {"_id": 0}).await.unwrap();
    check("deleteOne");
    assert_eq!(r.deleted_count, 1);

    // Upsert with a server-generated _id: the same id, one document.
    faults.arm("update", "rw");
    let r = c
        .update_one(doc! {"k": "u1"}, doc! {"$set": {"v": 1}})
        .upsert(true)
        .await
        .unwrap();
    check("upsert");
    let id = r.upserted_id.clone().unwrap();
    assert_eq!(ids(&c, doc! {"k": "u1"}).await, vec![id]);

    // insertMany, ordered, stopping at a duplicate: the same partial
    // result (two inserted, error at index 2), nothing after it.
    c.insert_one(doc! {"_id": 12}).await.unwrap();
    faults.arm("insert", "rw");
    let e = c
        .insert_many(vec![
            doc! {"_id": 10},
            doc! {"_id": 11},
            doc! {"_id": 12},
            doc! {"_id": 13},
        ])
        .await
        .unwrap_err();
    check("insertMany ordered, partial");
    assert_eq!(code_of(&e), 11000, "{e}");
    if let mongodb::error::ErrorKind::InsertMany(im) = e.kind.as_ref() {
        let we = im.write_errors.as_ref().unwrap();
        assert_eq!((we.len(), we[0].index), (1, 2), "{e}");
    } else {
        panic!("not an insertMany error: {e}");
    }
    assert_eq!(
        ids(&c, doc! {"_id": {"$gte": 10, "$lt": 20}}).await,
        vec![Bson::Int32(10), Bson::Int32(11), Bson::Int32(12)]
    );

    // insertMany, unordered, with a duplicate in the middle.
    faults.arm("insert", "rw");
    let e = c
        .insert_many(vec![doc! {"_id": 20}, doc! {"_id": 12}, doc! {"_id": 21}])
        .ordered(false)
        .await
        .unwrap_err();
    check("insertMany unordered, partial");
    assert_eq!(code_of(&e), 11000, "{e}");
    assert_eq!(
        ids(&c, doc! {"_id": {"$gte": 20, "$lt": 30}}).await,
        vec![Bson::Int32(20), Bson::Int32(21)]
    );

    // A bigger batch: 500 documents, then $inc on all of them through 50
    // single-document updates in one bulk (one update command).
    let docs: Vec<Document> = (100..600).map(|i| doc! {"_id": i, "v": 0}).collect();
    faults.arm("insert", "rw");
    let r = c.insert_many(docs).await.unwrap();
    check("insertMany of 500");
    assert_eq!(r.inserted_ids.len(), 500);
    assert_eq!(
        c.count_documents(doc! {"_id": {"$gte": 100, "$lt": 600}})
            .await
            .unwrap(),
        500
    );
    let r = c
        .delete_many(doc! {"_id": {"$gte": 100, "$lt": 600}})
        .await
        .unwrap();
    assert_eq!(r.deleted_count, 500);
    assert_eq!(
        c.count_documents(doc! {"_id": {"$gte": 100, "$lt": 600}})
            .await
            .unwrap(),
        0
    );
    db.drop().await.unwrap();
}

/// The gateway URI with another login (`<tenant>.<user>:<pwd>`).
fn as_user(u: &str, user: &str, pwd: &str) -> String {
    let (scheme, rest) = u.split_once("://").unwrap();
    let (cred, host) = rest.split_once('@').unwrap();
    let tenant = cred.split('.').next().unwrap();
    format!("{scheme}://{tenant}.{user}:{pwd}@{host}")
}

/// A user whose only grants are on single
/// collections still gets exactly-once retries. The record table is
/// reached through the backend grants on `__sprig_doc_sessions`
/// that every collection-level writer holds, not skipped.
#[tokio::test]
async fn collection_only_users_retry_once() {
    const DB: &str = "r5mongo3_retry_cu";
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let admin = Client::with_options(ClientOptions::parse(&u).await.unwrap()).unwrap();
    let data = admin.database(DB);
    let _ = data.drop().await;
    let _ = admin
        .database("admin")
        .run_command(doc! {"dropUser": "r5cu_writer"})
        .await;
    let _ = data.run_command(doc! {"dropRole": "r5cu_rw"}).await;
    data.create_collection("cw").await.unwrap();
    data.create_collection("other").await.unwrap();
    data.run_command(doc! {"createRole": "r5cu_rw", "privileges": [{"resource": {"db": DB, "collection": "cw"}, "actions": ["find", "insert", "update", "remove"]}], "roles": []})
        .await
        .unwrap();
    admin
        .database("admin")
        .run_command(doc! {"createUser": "r5cu_writer", "pwd": "pw-cu", "roles": [{"role": "r5cu_rw", "db": DB}]})
        .await
        .unwrap();

    let user_uri = as_user(&u, "r5cu_writer", "pw-cu");
    let target = ClientOptions::parse(&user_uri).await.unwrap();
    let host = target.hosts[0].to_string();
    let faults = Arc::new(Faults::default());
    let port = proxy(host.clone(), Arc::clone(&faults)).await;
    let mut o = ClientOptions::parse(user_uri.replacen(&host, &format!("127.0.0.1:{port}"), 1))
        .await
        .unwrap();
    o.app_name = Some("r5mongo3-retry-cu~dc-a".into());
    let client = Client::with_options(o).unwrap();
    let c = client.database(DB).collection::<Document>("cw");
    // Only the one collection: another of the same database is refused.
    let e = client
        .database(DB)
        .collection::<Document>("other")
        .insert_one(doc! {"_id": 1})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 13, "{e}");
    let mut cuts = 0;
    let mut check = |what: &str| {
        cuts += 1;
        assert_eq!(
            faults.fired.load(Ordering::SeqCst),
            cuts,
            "{what}: the proxy didn't cut a reply"
        );
        eprintln!("collection-only user, {what}: reply cut, retried, answered once");
    };

    faults.arm("insert", "cw");
    c.insert_one(doc! {"_id": 1, "v": 1}).await.unwrap();
    check("insertOne");
    assert_eq!(c.count_documents(doc! {"_id": 1}).await.unwrap(), 1);

    faults.arm("update", "cw");
    let r = c
        .update_one(doc! {"_id": 1}, doc! {"$inc": {"v": 1}})
        .await
        .unwrap();
    check("updateOne $inc");
    assert_eq!((r.matched_count, r.modified_count), (1, 1));
    let d = c.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("v").unwrap(), 2, "{d}");

    faults.arm("findAndModify", "cw");
    let d = c
        .find_one_and_update(doc! {"_id": 1}, doc! {"$inc": {"v": 10}})
        .return_document(ReturnDocument::After)
        .await
        .unwrap()
        .unwrap();
    check("findOneAndUpdate");
    assert_eq!(d.get_i32("v").unwrap(), 12, "{d}");
    let d = c.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("v").unwrap(), 12, "{d}");

    faults.arm("update", "cw");
    let r = c
        .update_one(doc! {"k": "u1"}, doc! {"$set": {"v": 1}})
        .upsert(true)
        .await
        .unwrap();
    check("upsert");
    assert_eq!(
        ids(&c, doc! {"k": "u1"}).await,
        vec![r.upserted_id.unwrap()]
    );

    faults.arm("delete", "cw");
    let r = c.delete_one(doc! {"_id": 1}).await.unwrap();
    check("deleteOne");
    assert_eq!(r.deleted_count, 1);

    admin
        .database("admin")
        .run_command(doc! {"dropUser": "r5cu_writer"})
        .await
        .unwrap();
    data.run_command(doc! {"dropRole": "r5cu_rw"})
        .await
        .unwrap();
    data.drop().await.unwrap();
}
