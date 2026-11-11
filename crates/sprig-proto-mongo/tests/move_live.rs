//! A tenant with MongoDB databases moved between clusters under writes
//! (chapter 10's mover, chapter 31's hidden tables; R5 round 6).
//!
//! Before the fix, the mover left a database's `__sprig_doc_*` tables
//! behind: after the move the next write to a database created before it
//! failed with 1146 (`__sprig_doc_sessions` missing), retry records and
//! transaction outcomes were lost, and so were search index definitions.
//!
//! - `SPRIG_TEST_MONGO_URI`: the gateway, as in `driver_live.rs`.
//! - `SPRIG_TEST_MOVE_CMDS`: commands separated by `;;` that each move the
//!   test tenant to another cluster and return once the cutover committed
//!   (such as `sprigctl ... shard move acme --to dc-a-c2 --wait`). Skipped
//!   without both.
//!
//! The test uses the database `r5six_mv` and drops it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::many_single_char_names
)]

use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::options::ClientOptions;
use mongodb::{Client, ClientSession, Collection, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn take(s: &mut ChangeStream<Document>, n: usize, secs: u64) -> Vec<Document> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while out.len() < n {
        match tokio::time::timeout_at(deadline, s.next()).await {
            Ok(Some(Ok(d))) => out.push(d),
            Ok(Some(Err(e))) => panic!("change stream failed: {e}"),
            Ok(None) | Err(_) => break,
        }
    }
    out
}

fn ids(evs: &[Document]) -> Vec<Bson> {
    evs.iter()
        .map(|e| {
            e.get_document("documentKey")
                .ok()
                .and_then(|k| k.get("_id").cloned())
                .unwrap_or(Bson::Null)
        })
        .collect()
}

/// The parts of a write reply a retry must repeat.
fn write_view(d: &Document) -> Document {
    let mut v = Document::new();
    for k in ["ok", "n", "nModified", "writeErrors", "value"] {
        if let Some(x) = d.get(k) {
            v.insert(k, x.clone());
        }
    }
    v
}

async fn run(db: &Database, s: &mut ClientSession, cmd: Document) -> Document {
    write_view(&db.run_command(cmd).session(s).await.unwrap())
}

fn vector(i: i32) -> Vec<f64> {
    let x = f64::from(i);
    vec![x, x * 0.5, -x, 1.0]
}

async fn nearest(c: &Collection<Document>, i: i32) -> Vec<Bson> {
    let stage = doc! {"index": "vx", "path": "emb", "queryVector": vector(i),
    "numCandidates": 50, "limit": 3};
    let got: Vec<Document> = c
        .aggregate(vec![
            doc! {"$vectorSearch": stage},
            doc! {"$project": {"_id": 1}},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    got.into_iter()
        .map(|d| d.get("_id").cloned().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mongo_databases_move_with_their_tenant() {
    let (Some(u), Some(cmds)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MOVE_CMDS")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MOVE_CMDS not set: skipped");
        return;
    };
    let mut o = ClientOptions::parse(&u).await.unwrap();
    o.app_name = Some("r5six-move~dc-a".into());
    let cl = Client::with_options(o).unwrap();
    let db = cl.database("r5six_mv");
    let _ = db.drop().await;
    let c: Collection<Document> = db.collection("docs");

    // Before the move: documents, a vector index with its side table, a
    // retry record, a committed transaction and an open change stream.
    let docs: Vec<Document> = (0..60)
        .map(|i| doc! {"_id": -1 - i, "v": 0, "emb": vector(i)})
        .collect();
    c.insert_many(docs).await.unwrap();
    let r = db
        .run_command(doc! {"createSearchIndexes": "docs", "indexes": [
        {"name": "vx", "type": "vectorSearch", "definition": {"fields": [
            {"type": "vector", "path": "emb", "numDimensions": 4, "similarity": "euclidean"}]}}]})
        .await
        .unwrap();
    assert_eq!(r.get_array("indexesCreated").unwrap().len(), 1, "{r}");
    let before = nearest(&c, 7).await;
    assert_eq!(before.first(), Some(&Bson::Int32(-8)), "{before:?}");

    // txnNumbers far above the driver's own: an explicit session reuses a
    // pooled server session that implicit writes may have used.
    let mut s = cl.start_session().await.unwrap();
    let inc = |n: i64| {
        doc! {"update": "docs", "updates": [{"q": {"_id": -1}, "u": {"$inc": {"v": 1}}}],
        "txnNumber": 1_000_000 + n}
    };
    let first = run(&db, &mut s, inc(1)).await;
    assert_eq!(
        (first.get_i32("n").ok(), first.get_i32("nModified").ok()),
        (Some(1), Some(1)),
        "{first}"
    );

    let mut t = cl.start_session().await.unwrap();
    t.start_transaction().await.unwrap();
    c.insert_one(doc! {"_id": -1000, "in": "txn before"})
        .session(&mut t)
        .await
        .unwrap();
    t.commit_transaction().await.unwrap();

    let mut stream = c.watch().await.unwrap().with_type::<Document>();

    // A writer of retryable inserts through every move and 50 inserts
    // past the last; a duplicate key on a retry means the first attempt
    // landed. Its errors are bounded: a database whose hidden tables
    // stayed behind fails every write.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = c.clone();
    let stop = std::sync::Arc::clone(&done);
    let writer = tokio::spawn(async move {
        let mut errors = 0u32;
        let mut last = String::new();
        let mut after_stop = 0;
        let mut i = 0i32;
        while after_stop < 50 {
            loop {
                match w.insert_one(doc! {"_id": i, "v": i}).await {
                    Ok(_) => break,
                    Err(e) if e.to_string().contains("E11000") => break,
                    Err(e) => {
                        errors += 1;
                        last = e.to_string();
                        assert!(errors < 600, "insert {i} keeps failing: {last}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            i += 1;
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                after_stop += 1;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (i, errors, last)
    });
    let moves: Vec<String> = cmds.split(";;").map(|x| x.trim().to_owned()).collect();
    for (k, m) in moves.iter().enumerate() {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let at = std::time::Instant::now();
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(m)
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "move failed: {}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        eprintln!("move {} committed in {:?}", k + 1, at.elapsed());
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let (n, errors, last) = writer.await.unwrap();
    eprintln!("writer: {n} inserts, {errors} retried errors (last: {last})");

    // After the move: the retry record answers the same and writes nothing.
    let again = run(&db, &mut s, inc(1)).await;
    assert_eq!(again, first, "the retry record didn't move");
    let d = c.find_one(doc! {"_id": -1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("v").unwrap(), 1, "a retried write ran twice: {d}");
    let next = run(&db, &mut s, inc(2)).await;
    assert_eq!(next.get_i32("nModified").ok(), Some(1), "{next}");
    let old = db.run_command(inc(1)).session(&mut s).await;
    assert!(
        old.is_err(),
        "an older txnNumber was accepted after the move"
    );

    // New writes: plain, retryable (implicit session) and in a transaction.
    c.insert_one(doc! {"_id": 10_000, "after": true})
        .await
        .unwrap();
    c.update_one(doc! {"_id": 10_000}, doc! {"$set": {"x": 1}})
        .await
        .unwrap();
    t.start_transaction().await.unwrap();
    c.insert_one(doc! {"_id": 10_001, "in": "txn after"})
        .session(&mut t)
        .await
        .unwrap();
    t.commit_transaction().await.unwrap();

    // The change stream saw every insert, in order, then the new writes.
    let evs = take(&mut stream, usize::try_from(n).unwrap() + 4, 120).await;
    let mut want: Vec<Bson> = (0..n).map(Bson::Int32).collect();
    want.extend([-1, 10_000, 10_000, 10_001].map(Bson::Int32));
    assert_eq!(ids(&evs), want, "the stream lost or repeated events");

    // The vector index (its definition and side table) came along.
    let listed: Vec<Document> = c
        .aggregate(vec![doc! {"$listSearchIndexes": {}}])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(nearest(&c, 7).await, before);
    assert_eq!(
        c.count_documents(doc! {}).await.unwrap(),
        60 + 1 + u64::try_from(n).unwrap() + 2
    );
    eprintln!(
        "after {} move(s): retry record kept, {} events in order, vector search unchanged",
        moves.len(),
        evs.len()
    );
    db.drop().await.unwrap();
}
