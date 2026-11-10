//! Collections in the tenant catalog (chapter 31, "Catalog tables"):
//! metadata survives moving from `__sprig_doc_meta` to the catalog, and
//! one gateway sees another's collections, indexes and array paths.
//!
//! - `SPRIG_TEST_MONGO_URI`: the gateway, as in `driver_live.rs`.
//! - `SPRIG_TEST_MONGO_URI2`: a second gateway of the same deployment
//!   (optional; the cross-gateway test is skipped without it).
//! - `SPRIG_TEST_LEGACY_PHASE`: `seed` writes the database
//!   `r5mongo2_legacy` (run it against a gateway that still keeps
//!   `__sprig_doc_meta`); `verify` checks it after the upgrade and drops
//!   it. Unset: the legacy test is skipped.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{ClientOptions, IndexOptions};
use mongodb::{Client, IndexModel};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn client(u: &str) -> Client {
    let mut o = ClientOptions::parse(u).await.unwrap();
    o.app_name = Some("r5mongo2-catalog~dc-a".into());
    Client::with_options(o).unwrap()
}

async fn sorted_ids(c: &mongodb::Collection<Document>, f: Document) -> Vec<Bson> {
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
async fn legacy_metadata_moves_to_the_catalog() {
    let (Some(u), Some(phase)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_LEGACY_PHASE"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_LEGACY_PHASE not set: skipped");
        return;
    };
    let db = client(&u).await.database("r5mongo2_legacy");
    let c = db.collection::<Document>("people");
    if phase == "seed" {
        let _ = db.drop().await;
        c.insert_many(vec![
            doc! {"_id": 1, "name": "a", "tags": ["x", "y"], "age": 30},
            doc! {"_id": 2, "name": "b", "tags": "x", "age": 40},
            doc! {"_id": 3, "name": "c", "age": 50},
        ])
        .await
        .unwrap();
        c.create_index(
            IndexModel::builder()
                .keys(doc! {"name": 1})
                .options(
                    IndexOptions::builder()
                        .unique(true)
                        .name("by_name".to_owned())
                        .build(),
                )
                .build(),
        )
        .await
        .unwrap();
        db.collection::<Document>("other")
            .insert_one(doc! {"_id": "o"})
            .await
            .unwrap();
        eprintln!("seeded r5mongo2_legacy");
        return;
    }
    // After the upgrade: both collections, the index (still unique), and
    // the array path (a query on tags finds the array element).
    let names = db.list_collection_names().await.unwrap();
    assert!(
        names.contains(&"people".to_owned()) && names.contains(&"other".to_owned()),
        "{names:?}"
    );
    let idx: Vec<String> = c.list_index_names().await.unwrap();
    assert_eq!(idx, vec!["_id_".to_owned(), "by_name".to_owned()]);
    assert_eq!(
        sorted_ids(&c, doc! {"tags": "x"}).await,
        vec![Bson::Int32(1), Bson::Int32(2)]
    );
    let e = c
        .insert_one(doc! {"_id": 4, "name": "a"})
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("11000") || e.to_string().contains("E11000"),
        "{e}"
    );
    // New writes after the move register new array paths in the catalog.
    c.insert_one(doc! {"_id": 5, "name": "e", "age": [1, 2]})
        .await
        .unwrap();
    assert_eq!(sorted_ids(&c, doc! {"age": 2}).await, vec![Bson::Int32(5)]);
    db.drop().await.unwrap();
}

#[tokio::test]
async fn two_gateways_share_collections() {
    let (Some(u1), Some(u2)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_URI2")) else {
        eprintln!("SPRIG_TEST_MONGO_URI2 not set: skipped");
        return;
    };
    let a = client(&u1).await.database("r5mongo2_two");
    let b = client(&u2).await.database("r5mongo2_two");
    let _ = a.drop().await;
    let ca = a.collection::<Document>("c");
    let cb = b.collection::<Document>("c");
    ca.insert_one(doc! {"_id": 1, "v": 1}).await.unwrap();
    // B sees the collection A created, and reads A's writes.
    assert!(
        b.list_collection_names()
            .await
            .unwrap()
            .contains(&"c".to_owned())
    );
    assert_eq!(sorted_ids(&cb, doc! {"v": 1}).await, vec![Bson::Int32(1)]);
    // A writes the first array at "v"; B's next query must see the path.
    ca.insert_one(doc! {"_id": 2, "v": [1, 7]}).await.unwrap();
    assert_eq!(sorted_ids(&cb, doc! {"v": 7}).await, vec![Bson::Int32(2)]);
    assert_eq!(
        sorted_ids(&cb, doc! {"v": 1}).await,
        vec![Bson::Int32(1), Bson::Int32(2)]
    );
    // An index created on B is used and listed on A.
    cb.create_index(IndexModel::builder().keys(doc! {"w": 1}).build())
        .await
        .unwrap();
    let names: Vec<String> = ca.list_index_names().await.unwrap();
    assert_eq!(names, vec!["_id_".to_owned(), "w_1".to_owned()]);
    // A drop on A is seen on B.
    ca.drop().await.unwrap();
    assert!(
        !b.list_collection_names()
            .await
            .unwrap()
            .contains(&"c".to_owned())
    );
    a.drop().await.unwrap();
}
