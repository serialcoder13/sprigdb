//! Live tests through the MongoDB Rust driver (chapter 30, "Testing").
//!
//! - `SPRIG_TEST_MONGO_URI`: a SprigDB gateway's MongoDB listener, with a
//!   tenant login, such as
//!   `mongodb://acme.app:sprig-dev-app@127.0.0.1:23717/?authSource=admin&directConnection=true`.
//! - `SPRIG_TEST_MONGO_REF_URI`: a reference MongoDB 8.0 for the
//!   differential suite, such as `mongodb://127.0.0.1:37018/?directConnection=true`.
//!
//! Each test is skipped when its variable is unset. The tests use the
//! databases `r5mongo_live` and `r5mongo_diff` and drop them at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap
)]

use std::str::FromStr;

use futures_util::TryStreamExt;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{
    Binary, Bson, DateTime, Decimal128, Document, JavaScriptCodeWithScope, Regex, Timestamp, doc,
    oid::ObjectId,
};
use mongodb::options::{ClientOptions, Compressor, IndexOptions};
use mongodb::{Client, Database, IndexModel};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn client(u: &str, comp: Option<Compressor>) -> Client {
    let mut o = ClientOptions::parse(u).await.unwrap();
    if let Some(c) = comp {
        o.compressors = Some(vec![c]);
    }
    o.app_name = Some("r5mongo-live~dc-a".into());
    Client::with_options(o).unwrap()
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
        mongodb::error::ErrorKind::Authentication { .. } => 18,
        _ => -1,
    }
}

/// Every BSON type a driver can write.
fn all_types() -> Document {
    doc! {
        "_id": ObjectId::parse_str("6721c5a4e1b2c3d4e5f60718").unwrap(),
        "double": 1.5, "doubleInt": 5.0, "negZero": -0.0, "nan": f64::NAN, "inf": f64::INFINITY,
        "string": "héllo \"wörld\"", "object": {"a": {"b": [1, [2, 3]]}},
        "array": [1, "two", 3.0, null, {"x": 1}], "empty": [], "emptyObj": {},
        "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![0, 1, 255] },
        "uuid": Binary { subtype: BinarySubtype::Uuid, bytes: (0..16).collect() },
        "oid": ObjectId::parse_str("6af1f2004528b7c68b5eb98a").unwrap(), "bool": true, "date": DateTime::from_millis(1_762_510_500_123),
        "oldDate": DateTime::from_millis(-62_198_755_200_000), "null": Bson::Null,
        "regex": Regex { pattern: "^a.*b$".into(), options: "i".into() },
        "js": Bson::JavaScriptCode("function() { return 1; }".into()),
        "jsScope": JavaScriptCodeWithScope { code: "x".into(), scope: doc! {"y": 1} },
        "int32": 42, "int64small": 7i64, "int64": 9_007_199_254_740_993i64,
        "ts": Timestamp { time: 1_700_000_000, increment: 7 },
        "decimal": Decimal128::from_str("59.90").unwrap(),
        "bigDecimal": Decimal128::from_str("1.2345E+6000").unwrap(),
        "minKey": Bson::MinKey, "maxKey": Bson::MaxKey,
        "symbol": Bson::Symbol("sym".into()), "undef": Bson::Undefined,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn gateway_live() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let c = client(&u, None).await;
    let admin = c.database("admin");
    let hello = admin.run_command(doc! {"hello": 1}).await.unwrap();
    assert_eq!(hello.get_str("msg").unwrap(), "isdbgrid");
    assert_eq!(hello.get_i32("maxWireVersion").unwrap(), 25);
    admin.run_command(doc! {"ping": 1}).await.unwrap();

    // Wrong password: 18.
    let bad = u.replacen(":sprig-dev-app@", ":wrong@", 1);
    let e = client(&bad, None)
        .await
        .database("admin")
        .run_command(doc! {"ping": 1})
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 18, "{e}");

    let db = c.database("r5mongo_live");
    let _ = db.drop().await;
    let coll = db.collection::<Document>("types");

    // Every BSON type round-trips through the type map.
    let d = all_types();
    coll.insert_one(d.clone()).await.unwrap();
    let back = coll
        .find_one(doc! {"_id": d.get("_id").unwrap()})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mongodb::bson::to_vec(&back).unwrap(),
        mongodb::bson::to_vec(&d).unwrap(),
        "\n{d}\n{back}"
    );

    // CRUD, cursors across getMore.
    let items = db.collection::<Document>("items");
    let docs: Vec<Document> = (0..250)
        .map(|i| doc! {"_id": i, "n": i, "cat": (["a", "b", "c"][usize::try_from(i % 3).unwrap()]), "tags": (if i % 10 == 0 { vec!["ten"] } else { vec![] })})
        .collect();
    let r = items.insert_many(docs).await.unwrap();
    assert_eq!(r.inserted_ids.len(), 250);
    let all: Vec<Document> = items
        .find(doc! {})
        .batch_size(40)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(all.len(), 250);
    let some: Vec<Document> = items
        .find(doc! {"cat": "b", "n": {"$gte": 100}})
        .sort(doc! {"n": -1})
        .skip(2)
        .limit(5)
        .projection(doc! {"_id": 0, "n": 1})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        some,
        vec![
            doc! {"n": 241},
            doc! {"n": 238},
            doc! {"n": 235},
            doc! {"n": 232},
            doc! {"n": 229}
        ]
    );
    assert_eq!(
        items.count_documents(doc! {"tags": "ten"}).await.unwrap(),
        25
    );
    assert_eq!(items.estimated_document_count().await.unwrap(), 250);
    let cats = items.distinct("cat", doc! {}).await.unwrap();
    assert_eq!(cats.len(), 3);
    let u1 = items
        .update_one(
            doc! {"_id": 1},
            doc! {"$inc": {"n": 1000}, "$set": {"x": true}},
        )
        .await
        .unwrap();
    assert_eq!((u1.matched_count, u1.modified_count), (1, 1));
    let um = items
        .update_many(doc! {"cat": "c"}, doc! {"$set": {"c": 1}})
        .await
        .unwrap();
    assert_eq!(um.matched_count, 83);
    let up = items
        .update_one(doc! {"_id": 1000}, doc! {"$set": {"n": 1}})
        .upsert(true)
        .await
        .unwrap();
    assert_eq!(up.upserted_id, Some(Bson::Int32(1000)));
    let rep = items
        .replace_one(doc! {"_id": 1000}, doc! {"replaced": true})
        .await
        .unwrap();
    assert_eq!(rep.modified_count, 1);
    let fam = items
        .find_one_and_update(doc! {"_id": 2}, doc! {"$set": {"n": -2}})
        .return_document(mongodb::options::ReturnDocument::After)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fam.get_i32("n").unwrap(), -2);
    let del = items.delete_many(doc! {"n": {"$lt": 10}}).await.unwrap();
    assert_eq!(
        del.deleted_count, 9,
        "n < 10 after the updates: 0, -2 and 3..9"
    );
    let agg: Vec<Document> = items
        .aggregate(vec![
            doc! {"$match": {"n": {"$lt": 200}}},
            doc! {"$group": {"_id": "$cat", "count": {"$sum": 1}}},
            doc! {"$sort": {"_id": 1}},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(agg.len(), 3);

    // Indexes: field columns plus a MariaDB index; unique duplicates fail.
    items
        .create_index(IndexModel::builder().keys(doc! {"cat": 1, "n": -1}).build())
        .await
        .unwrap();
    let names = items.list_index_names().await.unwrap();
    assert!(names.contains(&"cat_1_n_-1".to_owned()), "{names:?}");
    let e = items
        .create_index(
            IndexModel::builder()
                .keys(doc! {"cat": 1})
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .unwrap_err();
    assert_eq!(code_of(&e), 11000, "{e}");
    let e = items.insert_one(doc! {"_id": 20}).await.unwrap_err();
    assert_eq!(code_of(&e), 11000, "{e}");
    assert_eq!(
        items
            .count_documents(doc! {"cat": "a", "n": {"$gt": 200}})
            .await
            .unwrap(),
        17
    );
    items.drop_index("cat_1_n_-1").await.unwrap();

    // Transactions.
    let mut s = c.start_session().await.unwrap();
    s.start_transaction().await.unwrap();
    items
        .insert_one(doc! {"_id": "t1"})
        .session(&mut s)
        .await
        .unwrap();
    s.abort_transaction().await.unwrap();
    assert_eq!(items.count_documents(doc! {"_id": "t1"}).await.unwrap(), 0);
    s.start_transaction().await.unwrap();
    items
        .insert_one(doc! {"_id": "t2"})
        .session(&mut s)
        .await
        .unwrap();
    items
        .update_one(doc! {"_id": 20}, doc! {"$set": {"t": 1}})
        .session(&mut s)
        .await
        .unwrap();
    s.commit_transaction().await.unwrap();
    assert_eq!(
        items
            .count_documents(doc! {"$or": [{"_id": "t2"}, {"t": 1}]})
            .await
            .unwrap(),
        2
    );

    // Compression, each compressor.
    for comp in [
        Compressor::Zstd { level: None },
        Compressor::Snappy,
        Compressor::Zlib { level: None },
    ] {
        let cc = client(&u, Some(comp)).await;
        let n = cc
            .database("r5mongo_live")
            .collection::<Document>("items")
            .count_documents(doc! {})
            .await
            .unwrap();
        assert_eq!(n, 243, "250 + the upsert - 9 deleted + t2");
    }

    let dbs = c.list_database_names().await.unwrap();
    assert!(dbs.contains(&"r5mongo_live".to_owned()));
    let colls = db.list_collection_names().await.unwrap();
    assert_eq!(colls.len(), 2, "{colls:?}");
    db.drop().await.unwrap();
}

/// One step of the differential corpus: its name and result (or error
/// code).
async fn corpus(db: &Database) -> Vec<(String, Bson)> {
    let c = db.collection::<Document>("orders");
    let mut out: Vec<(String, Bson)> = Vec::new();
    macro_rules! step {
        ($name:expr, $e:expr) => {
            let r: Bson = match $e {
                Ok(v) => v,
                Err(e) => Bson::String(format!("error {}", code_of(&e))),
            };
            out.push(($name.to_owned(), r));
        };
    }
    let base: Vec<Document> = (0..60)
        .map(|i| {
            doc! {
                "_id": i, "status": (["paid", "new", "void", "shipped"][usize::try_from(i % 4).unwrap()]),
                "qty": (if i % 7 == 0 { Bson::Int64(i64::from(i)) } else if i % 5 == 0 { Bson::Double(f64::from(i) + 0.5) } else { Bson::Int32(i) }),
                "price": Decimal128::from_str(&format!("{}.{:02}", i, i % 100)).unwrap(),
                "customer": {"id": i % 6, "name": format!("c{}", i % 6)},
                "items": ((0..(i % 3)).map(|k| doc! {"sku": format!("S{k}"), "q": k + 1}).collect::<Vec<_>>()),
                "tags": (if i % 2 == 0 { vec!["even"] } else { vec!["odd", "x"] }),
                "when": DateTime::from_millis(1_700_000_000_000 + i64::from(i) * 86_400_000),
                "maybe": (if i % 9 == 0 { Bson::Null } else { Bson::Boolean(i % 2 == 0) }),
            }
        })
        .collect();
    step!(
        "insert",
        c.insert_many(base)
            .await
            .map(|r| Bson::Int64(i64::try_from(r.inserted_ids.len()).unwrap()))
    );
    let filters = [
        doc! {"status": "paid"},
        doc! {"qty": {"$gt": 20, "$lte": 40}},
        doc! {"qty": 14},
        doc! {"customer.id": 3},
        doc! {"items.sku": "S1"},
        doc! {"items": {"$size": 2}},
        doc! {"items": {"$elemMatch": {"sku": "S1", "q": 2}}},
        doc! {"tags": "x"},
        doc! {"tags": {"$all": ["odd", "x"]}},
        doc! {"maybe": null},
        doc! {"maybe": {"$exists": false}},
        doc! {"maybe": {"$type": "bool"}},
        doc! {"status": {"$in": ["new", "void"]}, "qty": {"$not": {"$gt": 30}}},
        doc! {"$or": [{"status": "void"}, {"customer.name": {"$regex": "^c[12]"}}]},
        doc! {"$nor": [{"status": "paid"}, {"tags": "x"}]},
        doc! {"price": {"$gte": Decimal128::from_str("30.30").unwrap()}},
        doc! {"when": {"$lt": DateTime::from_millis(1_700_000_000_000 + 10 * 86_400_000)}},
        doc! {"_id": {"$in": [1, 5, 99]}},
        doc! {"qty": {"$mod": [4, 1]}},
        doc! {"$expr": {"$gt": ["$qty", "$customer.id"]}},
        doc! {"status": {"$ne": "paid"}, "tags": {"$nin": ["x"]}},
    ];
    for (i, f) in filters.iter().enumerate() {
        step!(
            format!("find {i} {f}"),
            async {
                let v: Vec<Document> = c
                    .find(f.clone())
                    .sort(doc! {"_id": 1})
                    .await?
                    .try_collect()
                    .await?;
                Ok::<_, mongodb::error::Error>(Bson::Array(
                    v.into_iter().map(Bson::Document).collect(),
                ))
            }
            .await
        );
        step!(
            format!("count {i}"),
            c.count_documents(f.clone())
                .await
                .map(|n| Bson::Int64(i64::try_from(n).unwrap()))
        );
    }
    step!(
        "sort+limit",
        async {
            let v: Vec<Document> = c
                .find(doc! {})
                .sort(doc! {"status": 1, "qty": -1})
                .skip(3)
                .limit(10)
                .projection(doc! {"status": 1, "qty": 1})
                .await?
                .try_collect()
                .await?;
            Ok::<_, mongodb::error::Error>(Bson::Array(v.into_iter().map(Bson::Document).collect()))
        }
        .await
    );
    step!(
        "distinct",
        c.distinct("customer.name", doc! {"qty": {"$lt": 30}})
            .await
            .map(Bson::Array)
    );
    step!(
        "updateMany",
        c.update_many(
            doc! {"status": "new"},
            doc! {"$inc": {"qty": 1}, "$push": {"tags": "bumped"}, "$set": {"customer.vip": true}}
        )
        .await
        .map(|r| Bson::Document(doc! {"n": r.matched_count as i64, "m": r.modified_count as i64}))
    );
    step!(
        "updatePositional",
        c.update_one(
            doc! {"items.sku": "S1", "_id": 5},
            doc! {"$set": {"items.$.q": 99}}
        )
        .await
        .map(|r| Bson::Int64(r.modified_count as i64))
    );
    step!(
        "updateArrayFilters",
        c.update_many(doc! {}, doc! {"$inc": {"items.$[e].q": 10}})
            .array_filters(vec![doc! {"e.sku": "S0"}])
            .await
            .map(|r| Bson::Int64(r.modified_count as i64))
    );
    step!(
        "pullAddToSet",
        c.update_many(
            doc! {"tags": "x"},
            doc! {"$pull": {"tags": "x"}, "$addToSet": {"labels": {"$each": ["a", "b", "a"]}}}
        )
        .await
        .map(|r| Bson::Int64(r.modified_count as i64))
    );
    step!(
        "upsert",
        c.update_one(
            doc! {"_id": 500, "status": "fresh"},
            doc! {"$setOnInsert": {"qty": 1}}
        )
        .upsert(true)
        .await
        .map(|r| r.upserted_id.unwrap_or(Bson::Null))
    );
    step!(
        "immutable _id",
        c.update_one(doc! {"_id": 1}, doc! {"$set": {"_id": 2}})
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "conflict",
        c.update_one(doc! {"_id": 1}, doc! {"$set": {"a": 1}, "$inc": {"a": 1}})
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "inc on string",
        c.update_one(doc! {"_id": 1}, doc! {"$inc": {"status": 1}})
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "dup key",
        c.insert_one(doc! {"_id": 3}).await.map(|_| Bson::Null)
    );
    step!(
        "after updates",
        async {
            let v: Vec<Document> = c
                .find(doc! {})
                .sort(doc! {"_id": 1})
                .await?
                .try_collect()
                .await?;
            Ok::<_, mongodb::error::Error>(Bson::Array(v.into_iter().map(Bson::Document).collect()))
        }
        .await
    );
    let pipelines = [
        vec![
            doc! {"$match": {"qty": {"$gt": 5}}},
            doc! {"$group": {"_id": "$status", "n": {"$sum": 1}, "q": {"$sum": "$qty"}, "max": {"$max": "$qty"}, "avg": {"$avg": "$customer.id"}}},
            doc! {"$sort": {"_id": 1}},
        ],
        vec![
            doc! {"$unwind": "$items"},
            doc! {"$group": {"_id": "$items.sku", "total": {"$sum": "$items.q"}}},
            doc! {"$sort": {"_id": 1}},
        ],
        vec![
            doc! {"$project": {"_id": 1, "s": {"$concat": ["$status", "-", {"$toString": "$customer.id"}]}, "big": {"$cond": [{"$gte": ["$qty", 30]}, true, false]}}},
            doc! {"$sort": {"_id": -1}},
            doc! {"$limit": 5},
        ],
        vec![
            doc! {"$addFields": {"y": {"$year": "$when"}}},
            doc! {"$sortByCount": "$y"},
        ],
        vec![
            doc! {"$facet": {"a": [{"$count": "n"}], "b": [{"$match": {"status": "paid"}}, {"$project": {"_id": 1}}, {"$sort": {"_id": 1}}, {"$limit": 3}]}},
        ],
        vec![
            doc! {"$match": {"_id": {"$lt": 4}}},
            doc! {"$lookup": {"from": "orders", "localField": "customer.id", "foreignField": "_id", "as": "c"}},
            doc! {"$project": {"c.status": 1}},
            doc! {"$sort": {"_id": 1}},
        ],
    ];
    for (i, p) in pipelines.iter().enumerate() {
        step!(
            format!("aggregate {i}"),
            async {
                let v: Vec<Document> = c.aggregate(p.clone()).await?.try_collect().await?;
                Ok::<_, mongodb::error::Error>(Bson::Array(
                    v.into_iter().map(Bson::Document).collect(),
                ))
            }
            .await
        );
    }
    // Second collection: types, mixed sorts, array operators, errors.
    let m = db.collection::<Document>("mixed");
    step!(
        "insert types",
        m.insert_one(all_types()).await.map(|r| r.inserted_id)
    );
    step!(
        "read types",
        m.find_one(doc! {"decimal": Decimal128::from_str("59.90").unwrap()})
            .await
            .map(|d| d.map_or(Bson::Null, Bson::Document))
    );
    step!(
        "insert mixed",
        m.insert_many(vec![
            doc! {"_id": 1, "v": 5, "a": [3, 1, 2], "s": "b"},
            doc! {"_id": 2, "v": "x", "a": [9], "s": "a"},
            doc! {"_id": 3, "v": Bson::Null, "a": [], "s": "c"},
            doc! {"_id": 4, "a": [[1, 2], 0], "s": "a"},
            doc! {"_id": 5, "v": 2.5, "a": [5, 5], "s": "B"},
            doc! {"_id": 6, "v": {"k": 1}, "s": "é"},
            doc! {"_id": 7, "v": true, "a": "scalar"},
            doc! {"_id": 8, "v": DateTime::from_millis(5), "a": [{"z": 1}]},
        ])
        .await
        .map(|r| Bson::Int64(r.inserted_ids.len() as i64))
    );
    for (i, (f, sort)) in [
        (doc! {}, doc! {"v": 1, "_id": 1}),
        (doc! {}, doc! {"v": -1, "_id": 1}),
        (doc! {}, doc! {"a": 1, "_id": 1}),
        (doc! {}, doc! {"a": -1, "_id": 1}),
        (doc! {}, doc! {"s": 1, "_id": 1}),
        (doc! {"a": {"$gt": 2}}, doc! {"_id": 1}),
        (doc! {"a": [1, 2]}, doc! {"_id": 1}),
        (doc! {"a": {"$size": 0}}, doc! {"_id": 1}),
        (doc! {"a.0": 3}, doc! {"_id": 1}),
        (doc! {"v": {"$type": "number"}}, doc! {"_id": 1}),
        (doc! {"v": {"$exists": true, "$ne": null}}, doc! {"_id": 1}),
        (
            doc! {"s": {"$regex": "^b", "$options": "i"}},
            doc! {"_id": 1},
        ),
        (doc! {"s": {"$gte": "b"}}, doc! {"_id": 1}),
    ]
    .into_iter()
    .enumerate()
    {
        step!(
            format!("mixed {i} {f} {sort}"),
            async {
                let v: Vec<Document> = m
                    .find(f.clone())
                    .sort(sort.clone())
                    .projection(doc! {"_id": 1})
                    .await?
                    .try_collect()
                    .await?;
                Ok::<_, mongodb::error::Error>(Bson::Array(
                    v.into_iter().map(Bson::Document).collect(),
                ))
            }
            .await
        );
    }
    step!("proj slice elemMatch", async {
        let v: Vec<Document> = c.find(doc! {"_id": {"$lt": 6}}).sort(doc! {"_id": 1}).projection(doc! {"tags": {"$slice": 1}, "items": {"$elemMatch": {"q": {"$gt": 1}}}, "status": 1}).await?.try_collect().await?;
        Ok::<_, mongodb::error::Error>(Bson::Array(v.into_iter().map(Bson::Document).collect()))
    }.await);
    step!("push modifiers", m.update_one(doc! {"_id": 1}, doc! {"$push": {"a": {"$each": [7, 0], "$sort": 1, "$slice": -3, "$position": 0}}, "$pop": {"x": 1}}).await.map(|r| Bson::Int64(r.modified_count as i64)));
    step!("rename min max mul unset", m.update_one(doc! {"_id": 5}, doc! {"$rename": {"s": "t"}, "$min": {"v": 1}, "$max": {"w": 3}, "$mul": {"m": 2}, "$unset": {"a": ""}}).await.map(|r| Bson::Int64(r.modified_count as i64)));
    step!(
        "no-op update",
        m.update_one(doc! {"_id": 5}, doc! {"$set": {"w": 3}})
            .await
            .map(|r| Bson::Document(
                doc! {"n": r.matched_count as i64, "m": r.modified_count as i64}
            ))
    );
    step!(
        "upsert from query",
        m.update_one(
            doc! {"k": "new", "deep.x": 1, "n": {"$gt": 1}},
            doc! {"$set": {"set": true}}
        )
        .upsert(true)
        .await
        .map(|r| Bson::Boolean(r.upserted_id.is_some()))
    );
    step!(
        "upserted doc",
        // MongoDB 8.0 builds the seed in the query's order on most runs
        // and in path order on others (a fresh server), so the fields are
        // compared sorted.
        m.find_one(doc! {"k": "new"})
            .projection(doc! {"_id": 0})
            .await
            .map(|d| {
                d.map_or(Bson::Null, |d| {
                    let mut v: Vec<(String, Bson)> = d.into_iter().collect();
                    v.sort_by(|a, b| a.0.cmp(&b.0));
                    Bson::Document(v.into_iter().collect())
                })
            })
    );
    step!(
        "findOneAndUpdate before",
        m.find_one_and_update(doc! {"_id": 2}, doc! {"$set": {"v": "y"}})
            .await
            .map(|d| d.map_or(Bson::Null, Bson::Document))
    );
    step!(
        "findOneAndReplace",
        m.find_one_and_replace(doc! {"_id": 3}, doc! {"r": 1})
            .return_document(mongodb::options::ReturnDocument::After)
            .await
            .map(|d| d.map_or(Bson::Null, Bson::Document))
    );
    step!(
        "findOneAndDelete sorted",
        m.find_one_and_delete(doc! {"_id": {"$gt": 5}})
            .sort(doc! {"_id": -1})
            .await
            .map(|d| d.map_or(Bson::Null, Bson::Document))
    );
    step!(
        "distinct arrays",
        m.distinct("a", doc! {}).await.map(Bson::Array)
    );
    step!(
        "count skip limit",
        m.count_documents(doc! {})
            .skip(2)
            .limit(3)
            .await
            .map(|n| Bson::Int64(n as i64))
    );
    step!(
        "ordered insert dup",
        m.insert_many(vec![doc! {"_id": 100}, doc! {"_id": 1}, doc! {"_id": 101}])
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "unordered insert dup",
        m.insert_many(vec![doc! {"_id": 102}, doc! {"_id": 1}, doc! {"_id": 103}])
            .ordered(false)
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "after dups",
        m.count_documents(doc! {"_id": {"$gte": 100}})
            .await
            .map(|n| Bson::Int64(n as i64))
    );
    step!(
        "bad operator",
        m.find_one(doc! {"a": {"$bogus": 1}})
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "bad update",
        m.update_one(doc! {}, doc! {"$bogus": {"a": 1}})
            .await
            .map(|_| Bson::Null)
    );
    step!(
        "create exists",
        db.create_collection("mixed").await.map(|()| Bson::Null)
    );
    step!(
        "index list",
        async {
            m.create_index(IndexModel::builder().keys(doc! {"s": 1, "v": -1}).build())
                .await?;
            m.create_index(
                IndexModel::builder()
                    .keys(doc! {"k": 1})
                    .options(
                        IndexOptions::builder()
                            .name("kk".to_owned())
                            .sparse(true)
                            .build(),
                    )
                    .build(),
            )
            .await?;
            let v: Vec<mongodb::IndexModel> = m.list_indexes().await?.try_collect().await?;
            Ok::<_, mongodb::error::Error>(Bson::Array(
                v.into_iter().map(|i| Bson::Document(i.keys)).collect(),
            ))
        }
        .await
    );
    step!(
        "indexed query",
        async {
            let v: Vec<Document> = m
                .find(doc! {"s": "a"})
                .sort(doc! {"_id": 1})
                .projection(doc! {"_id": 1})
                .await?
                .try_collect()
                .await?;
            Ok::<_, mongodb::error::Error>(Bson::Array(v.into_iter().map(Bson::Document).collect()))
        }
        .await
    );
    step!(
        "drop missing index",
        m.drop_index("nope").await.map(|()| Bson::Null)
    );
    step!(
        "deleteOne",
        c.delete_one(doc! {"status": "void"})
            .await
            .map(|r| Bson::Int64(r.deleted_count as i64))
    );
    step!(
        "deleteMany",
        c.delete_many(doc! {"qty": {"$gte": 40}})
            .await
            .map(|r| Bson::Int64(r.deleted_count as i64))
    );
    step!(
        "final count",
        c.count_documents(doc! {})
            .await
            .map(|n| Bson::Int64(n as i64))
    );
    out.extend(bulk_corpus(db).await);
    out.extend(stream_corpus(db).await);
    out.extend(retry_corpus(db).await);
    out
}

/// Change streams (chapter 30 step 9): the events of a few writes, with
/// the resume token and times masked.
async fn stream_corpus(db: &Database) -> Vec<(String, Bson)> {
    use futures_util::StreamExt;
    let mut out: Vec<(String, Bson)> = Vec::new();
    let c = db.collection::<Document>("cs");
    c.insert_one(doc! {"_id": 0}).await.unwrap();
    let mut s = c
        .watch()
        .full_document(mongodb::options::FullDocumentType::UpdateLookup)
        .await
        .unwrap()
        .with_type::<Document>();
    c.insert_one(doc! {"_id": 1, "a": 1, "b": "x", "d": Decimal128::from_str("1.50").unwrap(), "t": [1], "o": {"p": 1, "q": 2}})
        .await
        .unwrap();
    c.update_one(
        doc! {"_id": 1},
        doc! {"$set": {"a": 2, "o.q": 3}, "$unset": {"b": ""}, "$push": {"t": 2}},
    )
    .await
    .unwrap();
    c.update_one(doc! {"_id": 1}, doc! {"$inc": {"a": 1}, "$pop": {"t": 1}})
        .await
        .unwrap();
    // A replacement: a `replace` event (r5-interim D14).
    c.replace_one(doc! {"_id": 1}, doc! {"r": 1, "s": "x"})
        .await
        .unwrap();
    c.delete_one(doc! {"_id": 1}).await.unwrap();
    let mut evs = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while evs.len() < 5 {
        match tokio::time::timeout_at(deadline, s.next()).await {
            Ok(Some(Ok(d))) => evs.push(d),
            _ => break,
        }
    }
    for (i, e) in evs.iter().enumerate() {
        let mut v = Document::new();
        for k in [
            "operationType",
            "ns",
            "documentKey",
            "fullDocument",
            "updateDescription",
        ] {
            // updateLookup: MongoDB reads the document when it sends the
            // event (null here, as it is deleted by then); SprigDB sends
            // the document as that update left it (r5-interim D14).
            if k == "fullDocument" && e.get_str("operationType") == Ok("update") {
                continue;
            }
            if let Some(x) = e.get(k) {
                v.insert(k, x.clone());
            }
        }
        // The order of updatedFields carries no meaning: compare sorted.
        if let Ok(ud) = v.get_document_mut("updateDescription")
            && let Ok(u) = ud.get_document("updatedFields")
        {
            let mut f: Vec<(String, Bson)> = u.clone().into_iter().collect();
            f.sort_by(|a, b| a.0.cmp(&b.0));
            ud.insert("updatedFields", f.into_iter().collect::<Document>());
        }
        out.push((format!("change event {i}"), Bson::Document(v)));
    }
    out.push((
        "change events".to_owned(),
        Bson::Int32(i32::try_from(evs.len()).unwrap()),
    ));
    out
}

/// The parts of a write reply the differential compares: counts,
/// upserted ids, write errors (index and code), `findAndModify`'s image.
fn write_view(d: &Document) -> Bson {
    let mut o = Document::new();
    for k in ["n", "nModified", "upserted", "value", "lastErrorObject"] {
        if let Some(v) = d.get(k) {
            o.insert(k, v.clone());
        }
    }
    if let Ok(w) = d.get_array("writeErrors") {
        let v: Vec<Bson> = w
            .iter()
            .filter_map(|e| e.as_document())
            .map(|e| Bson::Document(doc! {"index": e.get("index").cloned().unwrap_or(Bson::Null), "code": e.get("code").cloned().unwrap_or(Bson::Null)}))
            .collect();
        o.insert("writeErrors", v);
    }
    Bson::Document(o)
}

/// Retryable writes (chapter 30 step 7): the same `lsid` and `txnNumber`
/// sent twice answer the same and write once; an older `txnNumber` is
/// refused with 225. Needs a replica set on the reference side.
async fn retry_corpus(db: &Database) -> Vec<(String, Bson)> {
    let mut out: Vec<(String, Bson)> = Vec::new();
    // Runs last: the session's lsid, with txnNumbers chosen here, goes
    // back to the driver's pool, and a later implicit retryable write on
    // it would be too old.
    let client = db.client().clone();
    let mut s = client.start_session().await.unwrap();
    let c = db.collection::<Document>("retry");
    let mut run = async |name: &str, cmd: Document, s: &mut mongodb::ClientSession| {
        let r = match db.run_command(cmd).session(s).await {
            Ok(d) => write_view(&d),
            Err(e) => Bson::String(format!("error {}", code_of(&e))),
        };
        out.push((name.to_owned(), r));
    };
    let steps: Vec<(&str, Document)> = vec![
        (
            "retry insert",
            doc! {"insert": "retry", "documents": [{"_id": 1, "v": 0}], "txnNumber": 1001i64},
        ),
        (
            "retry insert again",
            doc! {"insert": "retry", "documents": [{"_id": 1, "v": 0}], "txnNumber": 1001i64},
        ),
        (
            "retry inc",
            doc! {"update": "retry", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"v": 1}}}], "txnNumber": 1002i64},
        ),
        (
            "retry inc again",
            doc! {"update": "retry", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"v": 1}}}], "txnNumber": 1002i64},
        ),
        (
            "retry too old",
            doc! {"insert": "retry", "documents": [{"_id": 2}], "txnNumber": 1001i64},
        ),
        (
            "retry upsert",
            doc! {"update": "retry", "updates": [{"q": {"_id": 3}, "u": {"$set": {"v": 3}}, "upsert": true}], "txnNumber": 1003i64},
        ),
        (
            "retry upsert again",
            doc! {"update": "retry", "updates": [{"q": {"_id": 3}, "u": {"$set": {"v": 3}}, "upsert": true}], "txnNumber": 1003i64},
        ),
        (
            "retry fam",
            doc! {"findAndModify": "retry", "query": {"_id": 1}, "update": {"$inc": {"v": 5}}, "new": true, "txnNumber": 1004i64},
        ),
        (
            "retry fam again",
            doc! {"findAndModify": "retry", "query": {"_id": 1}, "update": {"$inc": {"v": 5}}, "new": true, "txnNumber": 1004i64},
        ),
        (
            "retry partial",
            doc! {"insert": "retry", "documents": [{"_id": 10}, {"_id": 11}, {"_id": 1}, {"_id": 12}], "txnNumber": 1005i64},
        ),
        (
            "retry partial again",
            doc! {"insert": "retry", "documents": [{"_id": 10}, {"_id": 11}, {"_id": 1}, {"_id": 12}], "txnNumber": 1005i64},
        ),
        (
            "retry unordered",
            doc! {"insert": "retry", "documents": [{"_id": 20}, {"_id": 1}, {"_id": 21}], "ordered": false, "txnNumber": 1006i64},
        ),
        (
            "retry unordered again",
            doc! {"insert": "retry", "documents": [{"_id": 20}, {"_id": 1}, {"_id": 21}], "ordered": false, "txnNumber": 1006i64},
        ),
        (
            "retry delete",
            doc! {"delete": "retry", "deletes": [{"q": {"_id": 3}, "limit": 1}], "txnNumber": 1007i64},
        ),
        (
            "retry delete again",
            doc! {"delete": "retry", "deletes": [{"q": {"_id": 3}, "limit": 1}], "txnNumber": 1007i64},
        ),
    ];
    for (name, cmd) in steps {
        run(name, cmd, &mut s).await;
    }
    // bulkWrite as a retryable write (r5-interim D17): sent twice with
    // one txnNumber, it runs once and answers the same.
    let admin = client.database("admin");
    let ns = format!("{}.retry", db.name());
    for name in ["retry bulkWrite", "retry bulkWrite again"] {
        let cmd = doc! {"bulkWrite": 1, "ops": [{"insert": 0, "document": {"_id": 40}}, {"update": 0, "filter": {"_id": 40}, "updateMods": {"$inc": {"v": 1}}}, {"insert": 0, "document": {"_id": 40}}], "nsInfo": [{"ns": &ns}], "ordered": false, "txnNumber": 1008i64};
        let r = match admin.run_command(cmd).session(&mut s).await {
            Ok(d) => bulk_view(&d),
            Err(e) => Bson::String(format!("error {}", code_of(&e))),
        };
        out.push((name.to_owned(), r));
    }
    let all: Vec<Document> = match c.find(doc! {}).sort(doc! {"_id": 1}).await {
        Ok(cur) => cur.try_collect().await.unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    out.push((
        "retry state".to_owned(),
        Bson::Array(all.into_iter().map(Bson::Document).collect()),
    ));
    out
}

/// The parts of a `bulkWrite` reply the differential compares: the
/// totals and each result's `ok`, `idx`, counts, upserted id and code
/// (not the message).
fn bulk_view(d: &Document) -> Bson {
    let mut o = Document::new();
    for k in [
        "nErrors",
        "nInserted",
        "nMatched",
        "nModified",
        "nUpserted",
        "nDeleted",
        "ok",
    ] {
        if let Some(v) = d.get(k) {
            o.insert(k, v.clone());
        }
    }
    let items: Vec<Bson> = d
        .get_document("cursor")
        .ok()
        .and_then(|c| c.get_array("firstBatch").ok())
        .map(|a| {
            a.iter()
                .filter_map(Bson::as_document)
                .map(|r| {
                    let mut x = Document::new();
                    for k in [
                        "ok",
                        "idx",
                        "n",
                        "nModified",
                        "upserted",
                        "code",
                        "keyPattern",
                        "keyValue",
                    ] {
                        if let Some(v) = r.get(k) {
                            x.insert(k, v.clone());
                        }
                    }
                    Bson::Document(x)
                })
                .collect()
        })
        .unwrap_or_default();
    o.insert("results", items);
    if let Ok(c) = d.get_document("cursor") {
        o.insert("cursor id", c.get("id").cloned().unwrap_or(Bson::Null));
        o.insert("cursor ns", c.get("ns").cloned().unwrap_or(Bson::Null));
    }
    Bson::Document(o)
}

/// MongoDB 8.0's `bulkWrite` (r5-interim D17): mixed ops over two
/// collections, ordered and unordered, `errorsOnly`, refusals.
async fn bulk_corpus(db: &Database) -> Vec<(String, Bson)> {
    let admin = db.client().database("admin");
    let a = format!("{}.bw", db.name());
    let b = format!("{}.bw2", db.name());
    let steps: Vec<(&str, Document)> = vec![
        (
            "bulkWrite mixed unordered",
            doc! {"bulkWrite": 1, "ops": [
                {"insert": 0, "document": {"_id": 1, "a": 1}},
                {"insert": 0, "document": {"_id": 1}},
                {"update": 0, "filter": {"_id": 1}, "updateMods": {"$set": {"a": 2}}},
                {"update": 1, "filter": {"_id": 9}, "updateMods": {"b": 1}, "upsert": true},
                {"update": 0, "filter": {"_id": 1}, "updateMods": {"$set": {"_id": 2}}},
                {"delete": 0, "filter": {"_id": 1}, "multi": false},
            ], "nsInfo": [{"ns": &a}, {"ns": &b}], "ordered": false},
        ),
        (
            "bulkWrite ordered stops",
            doc! {"bulkWrite": 1, "ops": [
                {"insert": 0, "document": {"_id": 20}},
                {"insert": 0, "document": {"_id": 20}},
                {"insert": 0, "document": {"_id": 21}},
            ], "nsInfo": [{"ns": &a}]},
        ),
        (
            "bulkWrite errorsOnly",
            doc! {"bulkWrite": 1, "ops": [
                {"insert": 0, "document": {"_id": 30}},
                {"insert": 0, "document": {"_id": 30}},
            ], "nsInfo": [{"ns": &a}], "errorsOnly": true, "ordered": false},
        ),
        (
            "bulkWrite multi",
            doc! {"bulkWrite": 1, "ops": [
                {"update": 0, "filter": {}, "updateMods": {"$inc": {"m": 1}}, "multi": true},
                {"update": 0, "filter": {"_id": 404}, "updateMods": {"$inc": {"m": 1}}},
                {"delete": 1, "filter": {}, "multi": true},
            ], "nsInfo": [{"ns": &a}, {"ns": &b}]},
        ),
        (
            "bulkWrite empty",
            doc! {"bulkWrite": 1, "ops": [], "nsInfo": [{"ns": &a}]},
        ),
        (
            "bulkWrite bad nsInfo index",
            doc! {"bulkWrite": 1, "ops": [{"insert": 3, "document": {}}], "nsInfo": [{"ns": &a}]},
        ),
    ];
    let mut out = Vec::new();
    for (name, cmd) in steps {
        let r = match admin.run_command(cmd).await {
            Ok(d) => bulk_view(&d),
            Err(e) => Bson::String(format!("error {}", code_of(&e))),
        };
        out.push((name.to_owned(), r));
    }
    for coll in ["bw", "bw2"] {
        let all: Vec<Document> = match db
            .collection::<Document>(coll)
            .find(doc! {})
            .sort(doc! {"_id": 1})
            .await
        {
            Ok(cur) => cur.try_collect().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        out.push((
            format!("bulkWrite state {coll}"),
            Bson::Array(all.into_iter().map(Bson::Document).collect()),
        ));
    }
    out
}

/// Normalizes values whose MongoDB representation is implementation
/// detail: field order inside `$group` outputs is kept; numbers compare by
/// their BSON bytes.
fn same(a: &Bson, b: &Bson) -> bool {
    mongodb::bson::to_vec(&doc! {"v": a.clone()}).unwrap()
        == mongodb::bson::to_vec(&doc! {"v": b.clone()}).unwrap()
}

#[tokio::test]
async fn differential_against_mongodb() {
    let (Some(g), Some(r)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_REF_URI")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let gdb = client(&g, None).await.database("r5mongo_diff");
    let rdb = client(&r, None).await.database("r5mongo_diff");
    let _ = gdb.drop().await;
    let _ = rdb.drop().await;
    let a = corpus(&gdb).await;
    let b = corpus(&rdb).await;
    gdb.drop().await.unwrap();
    rdb.drop().await.unwrap();
    let mut fails = Vec::new();
    for ((name, x), (_, y)) in a.iter().zip(b.iter()) {
        if !same(x, y) {
            fails.push(format!("{name}\n  sprig: {x}\n  mongo: {y}"));
        }
    }
    let n = a.len();
    eprintln!("differential: {} of {n} steps equal", n - fails.len());
    assert!(
        fails.is_empty(),
        "{} of {n} steps differ:\n{}",
        fails.len(),
        fails.join("\n")
    );
}

/// Point reads by `_id`, one client, sequential: the p50 and p99 in
/// microseconds, printed for the gateway and (when set) the reference.
/// A probe, not a benchmark: run it against a release build on quiet
/// hardware for numbers worth comparing.
#[tokio::test]
async fn point_read_latency() {
    let Some(g) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let mut targets = vec![("sprig", g)];
    if let Some(r) = uri("SPRIG_TEST_MONGO_REF_URI") {
        targets.push(("mongo", r));
    }
    for (name, u) in targets {
        let db = client(&u, None).await.database("r5mongo_lat");
        let _ = db.drop().await;
        let c = db.collection::<Document>("kv");
        let docs: Vec<Document> = (0..1000)
            .map(|i| doc! {"_id": i, "field0": "x".repeat(100), "n": i})
            .collect();
        c.insert_many(docs).await.unwrap();
        for i in 0..200 {
            c.find_one(doc! {"_id": i}).await.unwrap();
        }
        let mut lat = Vec::with_capacity(2000);
        for i in 0..2000 {
            let t = std::time::Instant::now();
            let d = c.find_one(doc! {"_id": (i * 7) % 1000}).await.unwrap();
            lat.push(t.elapsed().as_micros());
            assert!(d.is_some());
        }
        lat.sort_unstable();
        eprintln!(
            "point reads {name}: p50 {} us, p99 {} us",
            lat[1000], lat[1980]
        );
        db.drop().await.unwrap();
    }
}
