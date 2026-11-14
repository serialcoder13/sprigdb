//! Row-level security on collections, live: policies made with `sprigCreatePolicy` bind every
//! MongoDB read and write of a bound user, SQL over the collection's
//! table, and change streams.
//!
//! The administrator (`<tenant>.app`) creates users `rlsm_alice` and
//! `rlsm_bob`, collection `notes` with 20 documents (owners alternate,
//! `n` = 1..20) and `refs` (one per note), and these policies:
//!
//! ```text
//! own    permissive  all  PUBLIC  using {owner: "$$USER"}  check {owner: "$$USER"}
//! small  restrictive all  PUBLIC  using {n: {$lt: 15}}
//! ```
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:33306
//! export SPRIG_TEST_RLS_MONGO=127.0.0.1:33737
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_mongo_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::BTreeSet;
use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;

const DB: &str = "rlsm_suite";
const PW: &str = "Rlsm-suite-pw-1";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if !cond {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

fn owner_of(i: i64) -> &'static str {
    if i % 2 == 0 {
        "rlsm_alice@%"
    } else {
        "rlsm_bob@%"
    }
}

/// The reference: what `user` may see of the seed.
fn visible(user: &str) -> BTreeSet<i64> {
    (1..=20)
        .filter(|i| owner_of(*i) == format!("{user}@%") && *i < 15)
        .collect()
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        _ => -1,
    }
}

fn ids(docs: &[Document]) -> BTreeSet<i64> {
    docs.iter()
        .filter_map(|d| match d.get("_id") {
            Some(Bson::Int32(i)) => Some(i64::from(*i)),
            Some(Bson::Int64(i)) => Some(*i),
            _ => None,
        })
        .collect()
}

async fn client(addr: &str, user: &str, pw: &str) -> mongodb::Client {
    let uri = format!(
        "mongodb://{}.{user}:{pw}@{addr}/?authSource=admin&directConnection=true",
        tenant()
    );
    mongodb::Client::with_uri_str(&uri).await.unwrap()
}

/// Up to `n` events of `stream` within `secs` seconds, as `op:_id`.
async fn collect_events(
    stream: &mut mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<Document>,
    >,
    n: usize,
    secs: u64,
) -> Vec<String> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while seen.len() < n && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), stream.next_if_any()).await {
            Ok(Ok(Some(ev))) => {
                let key = ev
                    .document_key
                    .as_ref()
                    .and_then(|k| k.get("_id").cloned())
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                seen.push(format!("{:?}:{key}", ev.operation_type));
            }
            Ok(Ok(None)) | Err(_) => {}
            Ok(Err(e)) => {
                seen.push(format!("error {e}"));
                break;
            }
        }
    }
    seen
}

async fn sql(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_level_security_on_collections() {
    let (Ok(sql_addr), Ok(mongo_addr)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_MONGO"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL and SPRIG_TEST_RLS_MONGO");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    // Setup: users (SQL), documents and policies (MongoDB, as the admin).
    let mut admin_sql = sql(&sql_addr, "app", "sprig-dev-app").await;
    for u in ["rlsm_alice", "rlsm_bob"] {
        let _ = admin_sql
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin_sql
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let admin = client(&mongo_addr, "app", "sprig-dev-app").await;
    let adb = admin.database(DB);
    let notes = adb.collection::<Document>("notes");
    let docs: Vec<Document> = (1..=20i64)
        .map(|i| doc! {"_id": i, "owner": owner_of(i), "n": i, "tags": [format!("t{}", i % 3)]})
        .collect();
    notes.insert_many(docs).await.unwrap();
    adb.collection::<Document>("refs")
        .insert_many(
            (1..=20i64)
                .map(|i| doc! {"_id": i, "note": i})
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    for u in ["rlsm_alice", "rlsm_bob"] {
        admin_sql
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
        admin_sql
            .query_drop(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON {DB}.* TO '{u}'@'%'"
            ))
            .await
            .unwrap();
    }
    // Policy commands and their refusals.
    let r = adb
        .run_command(doc! {"sprigCreatePolicy": "notes", "name": "own",
        "using": {"owner": "$$USER"}, "check": {"owner": "$$USER"}})
        .await;
    ck.ok("create own", r.is_ok(), || format!("{r:?}"));
    let r = adb
        .run_command(
            doc! {"sprigCreatePolicy": "notes", "name": "small", "mode": "restrictive",
            "using": {"n": {"$lt": 15}}},
        )
        .await;
    ck.ok("create small", r.is_ok(), || format!("{r:?}"));
    for (what, bad) in [
        ("regex", doc! {"owner": {"$regex": "a"}}),
        ("null", doc! {"owner": null}),
        ("nor", doc! {"$nor": [{"n": 1}]}),
    ] {
        let e = adb
            .run_command(doc! {"sprigCreatePolicy": "notes", "name": "bad", "using": bad})
            .await;
        ck.ok(
            &format!("refused: {what}"),
            e.as_ref().is_err_and(|e| code_of(e) == 39364),
            || format!("{e:?}"),
        );
    }
    let e = adb
        .run_command(doc! {"sprigCreatePolicy": "notes", "name": "own", "using": {"n": 1}})
        .await;
    ck.ok(
        "duplicate refused",
        e.as_ref().is_err_and(|e| code_of(e) == 1304),
        || format!("{e:?}"),
    );
    let r = adb
        .run_command(doc! {"sprigRowSecurity": "notes", "enable": true})
        .await;
    ck.ok("enable", r.is_ok(), || format!("{r:?}"));
    // The administrator manages the table (ALTER): it sees everything.
    let all: Vec<Document> = notes
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    ck.ok("manager sees all", all.len() == 20, || {
        format!("{}", all.len())
    });

    for user in ["rlsm_alice", "rlsm_bob"] {
        let want = visible(user);
        let c = client(&mongo_addr, user, PW).await;
        let db = c.database(DB);
        let coll = db.collection::<Document>("notes");
        let got: Result<Vec<Document>, _> = match coll.find(doc! {}).await {
            Ok(cur) => cur.try_collect().await,
            Err(e) => Err(e),
        };
        let got_ids = got.as_ref().map(|d| ids(d)).unwrap_or_default();
        ck.ok(&format!("{user} find"), got_ids == want, || {
            format!("{got_ids:?} vs {want:?} ({:?})", got.as_ref().err())
        });
        // A filter naming hidden documents finds none of them.
        let got: Vec<Document> = coll
            .find(doc! {"n": {"$gte": 10}})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let w: BTreeSet<i64> = want.iter().copied().filter(|i| *i >= 10).collect();
        ck.ok(&format!("{user} find filtered"), ids(&got) == w, || {
            format!("{:?} vs {w:?}", ids(&got))
        });
        let hidden = if user == "rlsm_alice" { 3 } else { 4 };
        let one = coll.find_one(doc! {"_id": hidden}).await.unwrap();
        ck.ok(
            &format!("{user} point read of a hidden document"),
            one.is_none(),
            || format!("{one:?}"),
        );
        let n = coll.count_documents(doc! {}).await.unwrap();
        ck.ok(
            &format!("{user} countDocuments"),
            n == want.len() as u64,
            || format!("{n}"),
        );
        let est = coll.estimated_document_count().await;
        ck.ok(
            &format!("{user} estimatedDocumentCount hides the rest"),
            est.as_ref().map_or(true, |n| *n <= want.len() as u64),
            || format!("{est:?}"),
        );
        let d = coll.distinct("owner", doc! {}).await.unwrap();
        ck.ok(
            &format!("{user} distinct"),
            d == vec![Bson::String(format!("{user}@%"))],
            || format!("{d:?}"),
        );
        let agg: Vec<Document> = coll
            .aggregate(vec![
                doc! {"$match": {"n": {"$gt": 0}}},
                doc! {"$group": {"_id": null, "c": {"$sum": 1}, "s": {"$sum": "$n"}}},
            ])
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let sum: i64 = want.iter().sum();
        let got_sum = agg.first().and_then(|d| d.get("s")).cloned();
        ck.ok(
            &format!("{user} aggregate"),
            got_sum.as_ref().is_some_and(|v| {
                matches!(v, Bson::Int32(x) if i64::from(*x) == sum)
                    || matches!(v, Bson::Int64(x) if *x == sum)
            }),
            || format!("{agg:?} vs {sum}"),
        );
        // $lookup from an unprotected collection into the protected one.
        let lk: Vec<Document> = db
            .collection::<Document>("refs")
            .aggregate(vec![
                doc! {"$lookup": {"from": "notes", "localField": "note", "foreignField": "_id", "as": "m"}},
                doc! {"$match": {"m.0": {"$exists": true}}},
            ])
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        ck.ok(&format!("{user} $lookup"), ids(&lk) == want, || {
            format!("{:?} vs {want:?}", ids(&lk))
        });
        // Writes touch visible documents only.
        let u = coll
            .update_many(doc! {}, doc! {"$set": {"seen": true}})
            .await
            .unwrap();
        ck.ok(
            &format!("{user} update_many"),
            u.matched_count == want.len() as u64,
            || format!("{u:?}"),
        );
        let dl = coll.delete_one(doc! {"_id": hidden}).await.unwrap();
        ck.ok(
            &format!("{user} delete hidden"),
            dl.deleted_count == 0,
            || format!("{dl:?}"),
        );
        let other = if user == "rlsm_alice" {
            "rlsm_bob@%"
        } else {
            "rlsm_alice@%"
        };
        let e = coll
            .insert_one(doc! {"_id": 100, "owner": other, "n": 1})
            .await;
        ck.ok(
            &format!("{user} insert for another owner refused"),
            e.as_ref().is_err_and(|e| code_of(e) == 39363),
            || format!("{e:?}"),
        );
        let id = if user == "rlsm_alice" { 101 } else { 102 };
        let e = coll
            .insert_one(doc! {"_id": id, "owner": format!("{user}@%"), "n": 1})
            .await;
        ck.ok(&format!("{user} insert own"), e.is_ok(), || {
            format!("{e:?}")
        });
        let e = coll
            .update_one(doc! {"_id": id}, doc! {"$set": {"owner": other}})
            .await;
        ck.ok(
            &format!("{user} update giving a document away refused"),
            e.as_ref().is_err_and(|e| code_of(e) == 39363),
            || format!("{e:?}"),
        );
        let e = coll.delete_one(doc! {"_id": id}).await;
        ck.ok(
            &format!("{user} delete own"),
            e.as_ref().is_ok_and(|d| d.deleted_count == 1),
            || format!("{e:?}"),
        );
        // Policy statements need ALTER.
        let e = db
            .run_command(doc! {"sprigDropPolicy": "notes", "name": "own"})
            .await;
        ck.ok(
            &format!("{user} drop policy refused"),
            e.as_ref().is_err_and(|e| matches!(code_of(e), 13 | 1142)),
            || format!("{e:?}"),
        );
        // SQL over the collection's table, the same rows.
        let mut s = sql(&sql_addr, user, PW).await;
        let n: Option<i64> = s
            .query_first(format!("SELECT COUNT(*) FROM {DB}.notes"))
            .await
            .unwrap();
        ck.ok(
            &format!("{user} SQL count"),
            n == Some(i64::try_from(want.len()).unwrap()),
            || format!("{n:?}"),
        );
    }

    // Change streams: alice sees her documents' events only; an
    // update that hides one arrives as a delete, one that shows one as an
    // insert.
    let alice = client(&mongo_addr, "rlsm_alice", PW).await;
    let mut stream = alice
        .database(DB)
        .collection::<Document>("notes")
        .watch()
        .await
        .unwrap();
    notes
        .insert_one(doc! {"_id": 200, "owner": "rlsm_alice@%", "n": 2})
        .await
        .unwrap();
    notes
        .insert_one(doc! {"_id": 201, "owner": "rlsm_bob@%", "n": 2})
        .await
        .unwrap();
    notes
        .update_one(doc! {"_id": 200}, doc! {"$set": {"n": 50}})
        .await
        .unwrap();
    notes
        .update_one(doc! {"_id": 201}, doc! {"$set": {"owner": "rlsm_alice@%"}})
        .await
        .unwrap();
    notes
        .update_one(doc! {"_id": 1}, doc! {"$set": {"x": 1}})
        .await
        .unwrap();
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while seen.len() < 3 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), stream.next_if_any()).await {
            Ok(Ok(Some(ev))) => {
                let key = ev
                    .document_key
                    .as_ref()
                    .and_then(|k| k.get("_id").cloned())
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                seen.push(format!("{:?}:{key}", ev.operation_type));
            }
            Ok(Ok(None)) | Err(_) => {}
            Ok(Err(e)) => {
                seen.push(format!("error {e}"));
                break;
            }
        }
    }
    let want = vec![
        "Insert:200".to_owned(),
        "Delete:200".to_owned(),
        "Insert:201".to_owned(),
    ];
    ck.ok("change stream events", seen == want, || format!("{seen:?}"));
    // A collection with a policy written in SQL can't be streamed.
    admin_sql
        .query_drop(format!(
            "CREATE POLICY sqlonly ON {DB}.refs USING (JSON_VALUE(doc, '$.note') < 5)"
        ))
        .await
        .unwrap();
    admin_sql
        .query_drop(format!("ALTER TABLE {DB}.refs ENABLE ROW LEVEL SECURITY"))
        .await
        .unwrap();
    let e = alice
        .database(DB)
        .collection::<Document>("refs")
        .watch()
        .await;
    ck.ok(
        "SQL-only policies: stream refused",
        e.as_ref().is_err_and(|e| code_of(e) == 39366),
        || format!("{:?}", e.as_ref().err()),
    );
    // A policy change reaches a stream that is already open. The
    // restrictive `small` (n < 15) hides 203 when the stream opens; once it
    // is dropped, the gateway ends the stream with a resumable error, the
    // driver resumes, and 203 arrives.
    let mut stream = alice
        .database(DB)
        .collection::<Document>("notes")
        .watch()
        .await
        .unwrap();
    // The stream's first getMore, under the policies it opened with.
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.next_if_any()).await;
    let r = adb
        .run_command(doc! {"sprigDropPolicy": "notes", "name": "small"})
        .await;
    ck.ok("drop small", r.is_ok(), || format!("{r:?}"));
    notes
        .insert_one(doc! {"_id": 203, "owner": "rlsm_alice@%", "n": 70})
        .await
        .unwrap();
    let seen = collect_events(&mut stream, 1, 20).await;
    ck.ok(
        "policy change reaches an open stream",
        seen == vec!["Insert:203".to_owned()],
        || format!("{seen:?}"),
    );
    // A database stream leaves out the events of collections the
    // watcher may not read (carol holds SELECT on notes only).
    let _ = admin_sql
        .query_drop("DROP USER IF EXISTS 'rlsm_carol'@'%'")
        .await;
    admin_sql
        .query_drop(format!("CREATE USER 'rlsm_carol'@'%' IDENTIFIED BY '{PW}'"))
        .await
        .unwrap();
    admin_sql
        .query_drop(format!("GRANT SELECT ON {DB}.notes TO 'rlsm_carol'@'%'"))
        .await
        .unwrap();
    adb.collection::<Document>("other")
        .insert_one(doc! {"_id": 1, "secret": "s1"})
        .await
        .unwrap();
    let carol = client(&mongo_addr, "rlsm_carol", PW).await;
    let mut dbs = carol.database(DB).watch().await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), dbs.next_if_any()).await;
    adb.collection::<Document>("other")
        .insert_one(doc! {"_id": 2, "secret": "s2"})
        .await
        .unwrap();
    notes
        .insert_one(doc! {"_id": 300, "owner": "rlsm_carol@%", "n": 1})
        .await
        .unwrap();
    adb.collection::<Document>("other")
        .insert_one(doc! {"_id": 3, "secret": "s3"})
        .await
        .unwrap();
    let seen = collect_events(&mut dbs, 3, 8).await;
    ck.ok(
        "database stream: unreadable collection left out",
        seen == vec!["Insert:300".to_owned()],
        || format!("{seen:?}"),
    );
    let _ = admin_sql
        .query_drop("DROP USER IF EXISTS 'rlsm_carol'@'%'")
        .await;
    // Disabling security opens the collection again.
    adb.run_command(doc! {"sprigRowSecurity": "notes", "enable": false})
        .await
        .unwrap();
    let n = alice
        .database(DB)
        .collection::<Document>("notes")
        .count_documents(doc! {})
        .await
        .unwrap();
    ck.ok("disabled", n == 24, || format!("{n}"));
    // Cleanup.
    let _ = admin_sql
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    for u in ["rlsm_alice", "rlsm_bob"] {
        let _ = admin_sql
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    eprintln!(
        "row-level security on collections: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
