//! Transaction outcomes across gateways: commit records and
//! recovery tokens.
//!
//! A proxy sits between the Rust driver and gateway 1. Armed with a
//! command, it either forwards the next such command and drops its reply
//! (the transaction committed; the driver saw a network error), or drops
//! the command itself (nothing committed). Either way it then sends every
//! new connection to gateway 2, so the driver's retry of
//! `commitTransaction`, which carries the recovery token, lands on a
//! gateway that never saw the transaction.
//!
//! - `SPRIG_TEST_MONGO_URI`: gateway 1, as in `driver_live.rs`.
//! - `SPRIG_TEST_MONGO_URI_2`: gateway 2 of the same deployment.
//!
//! Skipped unless both are set. Uses the database `r5mongo3_txn` and drops
//! it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};
use sprig_proto_mongo::codec;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Forward the command, drop its reply.
    LoseReply,
    /// Drop the command.
    LoseRequest,
}

struct Proxy {
    upstream: Mutex<String>,
    after: String,
    armed: Mutex<Option<(String, Fault)>>,
    fired: AtomicU32,
}

impl Proxy {
    /// The armed fault, if `msg` is its command (disarming it).
    fn hit(&self, msg: &[u8]) -> Option<Fault> {
        let mut g = self.armed.lock().unwrap();
        let (cmd, f) = g.as_ref()?;
        let Ok((_, codec::Frame::Msg(m), _)) = codec::decode(msg) else {
            return None;
        };
        let body = m.body.to_document().ok()?;
        if body.keys().next().map(String::as_str) != Some(cmd.as_str()) {
            return None;
        }
        let f = *f;
        *g = None;
        Some(f)
    }
}

async fn proxy(p: Arc<Proxy>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = l.accept().await else {
                return;
            };
            let p = Arc::clone(&p);
            tokio::spawn(async move {
                let up = p.upstream.lock().unwrap().clone();
                let Ok(server) = TcpStream::connect(&up).await else {
                    return;
                };
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Fault>(1);
                let p2 = Arc::clone(&p);
                let c2s = tokio::spawn(async move {
                    while let Ok(Some(m)) = codec::read_message(&mut cr).await {
                        match p2.hit(&m) {
                            Some(Fault::LoseRequest) => {
                                let _ = tx.send(Fault::LoseRequest).await;
                                return;
                            }
                            Some(Fault::LoseReply) => {
                                let _ = tx.send(Fault::LoseReply).await;
                            }
                            None => {}
                        }
                        if sw.write_all(&m).await.is_err() {
                            return;
                        }
                    }
                });
                let mut cut = false;
                loop {
                    tokio::select! {
                        f = rx.recv() => match f {
                            Some(Fault::LoseRequest) => {
                                p.fired.fetch_add(1, Ordering::SeqCst);
                                p.upstream.lock().unwrap().clone_from(&p.after);
                                break;
                            }
                            Some(Fault::LoseReply) => cut = true,
                            None => {}
                        },
                        m = codec::read_message(&mut sr) => {
                            let Ok(Some(m)) = m else { break };
                            if cut {
                                p.fired.fetch_add(1, Ordering::SeqCst);
                                p.upstream.lock().unwrap().clone_from(&p.after);
                                break;
                            }
                            if cw.write_all(&m).await.is_err() {
                                break;
                            }
                        }
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
        _ => -1,
    }
}

async fn count(c: &Collection<Document>, f: Document) -> u64 {
    c.count_documents(f).await.unwrap()
}

#[tokio::test]
async fn commits_are_recovered_on_another_gateway() {
    let (Some(u1), Some(u2)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_URI_2")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_URI_2 not set: skipped");
        return;
    };
    let h1 = ClientOptions::parse(&u1).await.unwrap().hosts[0].to_string();
    let h2 = ClientOptions::parse(&u2).await.unwrap().hosts[0].to_string();
    let direct = Client::with_options(ClientOptions::parse(&u1).await.unwrap()).unwrap();
    let db = direct.database("r5mongo3_txn");
    let _ = db.drop().await;
    db.create_collection("t").await.unwrap();
    let check = direct.database("r5mongo3_txn").collection::<Document>("t");

    for (round, fault) in [(1, Fault::LoseReply), (2, Fault::LoseRequest)] {
        let p = Arc::new(Proxy {
            upstream: Mutex::new(h1.clone()),
            after: h2.clone(),
            armed: Mutex::new(None),
            fired: AtomicU32::new(0),
        });
        let port = proxy(Arc::clone(&p)).await;
        let mut o = ClientOptions::parse(u1.replacen(&h1, &format!("127.0.0.1:{port}"), 1))
            .await
            .unwrap();
        o.app_name = Some("r5mongo3-txn~dc-a".into());
        let client = Client::with_options(o).unwrap();
        let c = client.database("r5mongo3_txn").collection::<Document>("t");
        let mut s = client.start_session().await.unwrap();
        s.start_transaction().await.unwrap();
        c.insert_one(doc! {"_id": round * 10, "r": round})
            .session(&mut s)
            .await
            .unwrap();
        c.insert_one(doc! {"_id": round * 10 + 1, "r": round})
            .session(&mut s)
            .await
            .unwrap();
        *p.armed.lock().unwrap() = Some(("commitTransaction".into(), fault));
        let r = s.commit_transaction().await;
        assert_eq!(
            p.fired.load(Ordering::SeqCst),
            1,
            "round {round}: no fault fired"
        );
        match fault {
            Fault::LoseReply => {
                // Committed on gateway 1; gateway 2 answers from the record.
                r.unwrap();
                assert_eq!(count(&check, doc! {"r": round}).await, 2);
                eprintln!("commit reply lost: the retry on gateway 2 answered ok from the record");
            }
            Fault::LoseRequest => {
                // Never committed: gateway 2 fences it and answers 251.
                let e = r.unwrap_err();
                assert_eq!(code_of(&e), 251, "{e}");
                assert!(e.contains_label("TransientTransactionError"), "{e}");
                assert_eq!(count(&check, doc! {"r": round}).await, 0);
                eprintln!("commit request lost: gateway 2 answered 251 and fenced the transaction");
            }
        }
    }
    db.drop().await.unwrap();
}
