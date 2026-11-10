//! Change streams through the Rust driver (chapter 30 step 9, chapter 38
//! step 14; chapter 48's R5 done-when: "change streams resume correctly
//! across shard moves").
//!
//! - `SPRIG_TEST_MONGO_URI`: the gateway, as in `driver_live.rs`.
//! - `SPRIG_TEST_MOVE_CMDS`: commands separated by `;;` that each move the
//!   test tenant to another cluster and return once the cutover
//!   committed (such as `sprigctl ... shard move acme --to dc-a-c2
//!   --wait`). The move test is skipped without it.
//!
//! The tests use the databases `r5mongo2_cs`, `r5mongo3_cs_replace` and
//! `r5mongo2_cs_move` and drop them at the end.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_wrap
)]

use std::str::FromStr;
use std::time::Duration;

use futures_util::StreamExt;
use mongodb::bson::{Bson, DateTime, Decimal128, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{ClientOptions, FullDocumentType};
use mongodb::{Client, Collection};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn client(u: &str) -> Client {
    let mut o = ClientOptions::parse(u).await.unwrap();
    o.app_name = Some("r5mongo2-changes~dc-a".into());
    Client::with_options(o).unwrap()
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

#[tokio::test]
async fn change_stream_events_and_resume() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let db = client(&u).await.database("r5mongo2_cs");
    let _ = db.drop().await;
    let c: Collection<Document> = db.collection("events");
    c.insert_one(doc! {"_id": 0}).await.unwrap();
    let mut s = c
        .watch()
        .full_document(FullDocumentType::UpdateLookup)
        .await
        .unwrap()
        .with_type::<Document>();
    let when = DateTime::from_millis(1_762_510_500_123);
    let price = Decimal128::from_str("59.90").unwrap();
    c.insert_one(doc! {"_id": 1, "a": 1, "b": "x", "price": price, "when": when, "tags": ["t"]})
        .await
        .unwrap();
    c.insert_one(doc! {"_id": 2, "a": 2}).await.unwrap();
    c.update_one(
        doc! {"_id": 1},
        doc! {"$set": {"a": 5}, "$unset": {"b": ""}, "$push": {"tags": "u"}},
    )
    .await
    .unwrap();
    c.delete_one(doc! {"_id": 2}).await.unwrap();
    let evs = take(&mut s, 4, 30).await;
    let kinds: Vec<&str> = evs
        .iter()
        .map(|e| e.get_str("operationType").unwrap())
        .collect();
    assert_eq!(
        kinds,
        vec!["insert", "insert", "update", "delete"],
        "{evs:?}"
    );
    assert_eq!(
        ids(&evs),
        vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(1),
            Bson::Int32(2)
        ]
    );
    // Types survive: Decimal128 and dates come back as written.
    let full = evs[0].get_document("fullDocument").unwrap();
    assert_eq!(full.get("price"), Some(&Bson::Decimal128(price)));
    assert_eq!(full.get("when"), Some(&Bson::DateTime(when)));
    assert_eq!(
        evs[0].get_document("ns").unwrap(),
        &doc! {"db": "r5mongo2_cs", "coll": "events"}
    );
    let ud = evs[2].get_document("updateDescription").unwrap();
    assert_eq!(
        ud.get_document("updatedFields").unwrap(),
        &doc! {"a": 5, "tags.1": "u"}
    );
    assert_eq!(
        ud.get_array("removedFields").unwrap(),
        &vec![Bson::String("b".into())]
    );
    // updateLookup: the document as the update left it.
    assert_eq!(
        evs[2]
            .get_document("fullDocument")
            .unwrap()
            .get_i32("a")
            .unwrap(),
        5
    );
    // Resume after the second event: the update and the delete again,
    // then what came after, nothing else.
    let token: ResumeToken = mongodb::bson::from_bson(evs[1].get("_id").cloned().unwrap()).unwrap();
    drop(s);
    c.insert_one(doc! {"_id": 3}).await.unwrap();
    let mut r = c
        .watch()
        .resume_after(token)
        .await
        .unwrap()
        .with_type::<Document>();
    let again = take(&mut r, 3, 30).await;
    assert_eq!(
        ids(&again),
        vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]
    );
    assert_eq!(
        again
            .iter()
            .map(|e| e.get("_id").cloned().unwrap())
            .collect::<Vec<_>>()[..2],
        evs.iter()
            .map(|e| e.get("_id").cloned().unwrap())
            .collect::<Vec<_>>()[2..]
    );
    // A database stream with a $match: inserts only, from two collections.
    let mut d = db
        .watch()
        .pipeline(vec![doc! {"$match": {"operationType": "insert"}}])
        .await
        .unwrap()
        .with_type::<Document>();
    c.insert_one(doc! {"_id": 10}).await.unwrap();
    c.delete_one(doc! {"_id": 10}).await.unwrap();
    db.collection::<Document>("other")
        .insert_one(doc! {"_id": 11})
        .await
        .unwrap();
    let evs = take(&mut d, 2, 30).await;
    let ns: Vec<String> = evs
        .iter()
        .map(|e| {
            e.get_document("ns")
                .unwrap()
                .get_str("coll")
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(ns, vec!["events".to_owned(), "other".to_owned()]);
    // Dropping the watched collection ends its stream (after the insert
    // and delete of _id 10, which the resumed stream also saw).
    c.drop().await.unwrap();
    let end = take(&mut r, 4, 30).await;
    let kinds: Vec<&str> = end
        .iter()
        .map(|e| e.get_str("operationType").unwrap())
        .collect();
    assert_eq!(
        kinds,
        vec!["insert", "delete", "drop", "invalidate"],
        "{end:?}"
    );
    db.drop().await.unwrap();
}

/// D14 (decisions/r5-interim.md): replacements are `replace` events,
/// with the new document and no update description, as MongoDB sends
/// them; operator updates stay `update`, and an upserting replacement
/// that inserts is an `insert`.
#[tokio::test]
async fn replacements_are_replace_events() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let db = client(&u).await.database("r5mongo3_cs_replace");
    let _ = db.drop().await;
    let c: Collection<Document> = db.collection("r");
    c.insert_one(doc! {"_id": 1, "a": 1}).await.unwrap();
    let mut s = c.watch().await.unwrap().with_type::<Document>();
    let when = DateTime::from_millis(1_762_510_500_123);
    c.replace_one(doc! {"_id": 1}, doc! {"b": "x", "when": when})
        .await
        .unwrap();
    c.update_one(doc! {"_id": 1}, doc! {"$set": {"c": 1}})
        .await
        .unwrap();
    c.find_one_and_replace(doc! {"_id": 1}, doc! {"d": 2})
        .await
        .unwrap();
    c.replace_one(doc! {"_id": 2}, doc! {"e": 3})
        .upsert(true)
        .await
        .unwrap();
    let evs = take(&mut s, 4, 30).await;
    let kinds: Vec<&str> = evs
        .iter()
        .map(|e| e.get_str("operationType").unwrap())
        .collect();
    assert_eq!(
        kinds,
        vec!["replace", "update", "replace", "insert"],
        "{evs:?}"
    );
    assert_eq!(
        evs[0].get_document("fullDocument").unwrap(),
        &doc! {"_id": 1, "b": "x", "when": when}
    );
    assert!(!evs[0].contains_key("updateDescription"), "{:?}", evs[0]);
    assert_eq!(
        evs[0].get_document("documentKey").unwrap(),
        &doc! {"_id": 1}
    );
    assert!(evs[1].contains_key("updateDescription"));
    assert_eq!(
        evs[2].get_document("fullDocument").unwrap(),
        &doc! {"_id": 1, "d": 2}
    );
    db.drop().await.unwrap();
}

#[tokio::test]
async fn change_stream_survives_shard_moves() {
    let (Some(u), Some(cmds)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MOVE_CMDS")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MOVE_CMDS not set: skipped");
        return;
    };
    let cl = client(&u).await;
    let db = cl.database("r5mongo2_cs_move");
    let _ = db.drop().await;
    let c: Collection<Document> = db.collection("m");
    c.insert_one(doc! {"_id": -1}).await.unwrap();
    let mut s = c.watch().await.unwrap().with_type::<Document>();
    let moves: Vec<String> = cmds.split(";;").map(|x| x.trim().to_owned()).collect();
    let n = 400i32;
    let per_move = n / i32::try_from(moves.len() + 1).unwrap();
    // A writer inserting 0..n one at a time, retrying through the holds;
    // a duplicate key on a retry means the first attempt landed.
    let w = c.clone();
    let writer = tokio::spawn(async move {
        let mut errors = 0;
        for i in 0..n {
            loop {
                match w.insert_one(doc! {"_id": i, "v": i}).await {
                    Ok(_) => break,
                    Err(e) if e.to_string().contains("E11000") => break,
                    Err(_) => {
                        errors += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        errors
    });
    let mut got: Vec<Document> = Vec::new();
    let mut token_mid = None;
    for (k, m) in moves.iter().enumerate() {
        let want = per_move * i32::try_from(k + 1).unwrap();
        let more = take(&mut s, usize::try_from(want).unwrap() - got.len(), 120).await;
        got.extend(more);
        if k == 0 {
            token_mid = got.last().and_then(|e| e.get("_id").cloned());
        }
        let t = std::time::Instant::now();
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
        eprintln!("move {} committed in {:?}", k + 1, t.elapsed());
    }
    let errors = writer.await.unwrap();
    let rest = take(&mut s, usize::try_from(n).unwrap() - got.len(), 120).await;
    got.extend(rest);
    let want: Vec<Bson> = (0..n).map(Bson::Int32).collect();
    assert_eq!(ids(&got), want, "the live stream lost or repeated events");
    eprintln!(
        "live stream: {} inserts across {} moves, in order, none lost or repeated; {errors} writer retries",
        got.len(),
        moves.len()
    );
    // Resuming from a token taken before the first move gives the rest,
    // exactly once, from the cluster the tenant is on now.
    let token: ResumeToken = mongodb::bson::from_bson(token_mid.unwrap()).unwrap();
    let mut r = c
        .watch()
        .resume_after(token)
        .await
        .unwrap()
        .with_type::<Document>();
    let k = usize::try_from(per_move).unwrap();
    let again = take(&mut r, usize::try_from(n).unwrap() - k, 120).await;
    assert_eq!(
        ids(&again),
        want[k..].to_vec(),
        "the resumed stream lost or repeated events"
    );
    eprintln!(
        "resumed stream: {} events after the token, exactly once",
        again.len()
    );
    db.drop().await.unwrap();
}
