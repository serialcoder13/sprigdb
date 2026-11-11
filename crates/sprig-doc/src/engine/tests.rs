//! Unit tests, and a live test of the generated SQL directly on MariaDB
//! (no gateway), run when `SPRIG_TEST_DOC_MARIADB` names a server, such
//! as `mysql://root:sprig-dev-root@127.0.0.1:13301`. It uses the database
//! `r5mongo_doctest` and drops it at the end.

use std::str::FromStr;

use bson::{Bson, Decimal128, doc};
use mysql_async::prelude::Queryable;

use super::*;

#[test]
fn array_paths_of_documents() {
    let d = doc! {"a": [1, {"b": [2]}], "c": {"d": []}, "e": 1};
    let p: Vec<String> = array_paths(&d).into_iter().collect();
    assert_eq!(p, vec!["a", "a.b", "c.d"]);
    assert!(hlc_now() < hlc_now());
    assert!(check_coll_name("system.x").is_err());
    assert!(check_db_name("a.b").is_err());
}

struct Direct(mysql_async::Conn);

fn val(v: mysql_async::Value) -> Option<Vec<u8>> {
    match v {
        mysql_async::Value::NULL => None,
        mysql_async::Value::Bytes(b) => Some(b),
        other => Some(other.as_sql(true).into_bytes()),
    }
}

fn err(e: &mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message.clone(),
            mongo: None,
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
            mongo: None,
        },
    }
}

#[async_trait]
impl SqlRunner for Direct {
    async fn query(&mut self, sql: &str) -> Result<Rows, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(|e| err(&e))?;
        Ok(rows
            .into_iter()
            .map(|r| r.unwrap().into_iter().map(val).collect())
            .collect())
    }
    async fn exec(&mut self, sql: &str) -> Result<Done, SqlError> {
        self.0.query_drop(sql).await.map_err(|e| err(&e))?;
        Ok(Done {
            affected: self.0.affected_rows(),
            info: self.0.info().into_owned(),
        })
    }
}

async fn all(e: &Engine, s: &mut Direct, ns: &Ns, spec: FindSpec) -> Vec<Document> {
    let mut plan = e.find(s, ns, spec).await.unwrap();
    let mut out = Vec::new();
    loop {
        let b = e.next_batch(s, &mut plan, 2, 1 << 20).await.unwrap();
        out.extend(b);
        if plan.exhausted {
            return out;
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_engine_on_mariadb() {
    let Ok(url) = std::env::var("SPRIG_TEST_DOC_MARIADB") else {
        eprintln!("SPRIG_TEST_DOC_MARIADB not set: skipped");
        return;
    };
    let conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    let mut s = Direct(conn);
    let e = Engine::default();
    let db = "r5mongo_doctest";
    let _ = s.exec(&format!("DROP DATABASE IF EXISTS {db}")).await;
    let ns = Ns {
        db: db.into(),
        coll: "orders".into(),
    };
    let oid = ObjectId::parse_str("6721c5a4e1b2c3d4e5f60718").unwrap();
    let docs = vec![
        doc! {"_id": oid, "status": "paid", "qty": 3i64, "total": Decimal128::from_str("59.90").unwrap(),
        "created": bson::DateTime::from_millis(1_762_510_500_000), "customer": {"id": 42},
        "items": [{"sku": "A-1", "price": Decimal128::from_str("19.95").unwrap()}, {"sku": "B-7", "price": 39.95}]},
        doc! {"_id": 2, "status": "new", "qty": 1, "tags": ["x", "y"], "customer": {"id": 7}},
        doc! {"_id": 3, "status": "paid", "qty": 10, "customer": {"id": 42}, "note": Bson::Null},
        doc! {"_id": "four", "status": "void", "qty": 2.5},
    ];
    let (n, errs) = e.insert(&mut s, &ns, docs.clone(), true).await.unwrap();
    assert_eq!((n, errs.len()), (4, 0));
    // types round-trip through MariaDB
    let got = all(
        &e,
        &mut s,
        &ns,
        FindSpec {
            filter: doc! {"_id": oid},
            ..Default::default()
        },
    )
    .await;
    assert_eq!(got, vec![docs[0].clone()]);
    // R5 round 8: a point read's BSON written from the stored JSON is the
    // document's own; a miss is `Some(None)`; other filters aren't points.
    for d in &docs {
        let raw = e
            .find_point_raw(&mut s, &ns, &doc! {"_id": d.get("_id").unwrap().clone()})
            .await
            .unwrap();
        assert_eq!(raw, Some(Some(bson::to_vec(d).unwrap())), "{d}");
    }
    let miss = e
        .find_point_raw(&mut s, &ns, &doc! {"_id": 99})
        .await
        .unwrap();
    assert_eq!(miss, Some(None));
    let not_point = e
        .find_point_raw(&mut s, &ns, &doc! {"status": "paid"})
        .await
        .unwrap();
    assert_eq!(not_point, None);
    // duplicate key
    let (n, errs) = e
        .insert(&mut s, &ns, vec![doc! {"_id": 2}], true)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(errs[0].1.code, code::DUPLICATE_KEY);
    // the registry learned the array paths
    let m = e.meta(&mut s, &ns).await.unwrap().unwrap();
    assert!(m.array_paths.contains("items") && m.array_paths.contains("tags"));
    let count = |f: Document| {
        let f = f.clone();
        async move { f }
    };
    let _ = count;
    for (f, want) in [
        (doc! {"status": "paid"}, 2u64),
        (doc! {"customer.id": 42}, 2),
        (doc! {"qty": {"$gt": 2}}, 3),
        (doc! {"qty": {"$gte": 2.5, "$lt": 10}}, 2),
        (doc! {"tags": "y"}, 1),
        (doc! {"items.sku": "B-7"}, 1),
        (doc! {"items.price": 39.95}, 1),
        (doc! {"note": Bson::Null}, 4),
        (doc! {"note": {"$exists": true}}, 1),
        (doc! {"status": {"$in": ["new", "void"]}}, 2),
        (doc! {"status": {"$ne": "paid"}}, 2),
        (
            doc! {"created": {"$gte": bson::DateTime::from_millis(1_700_000_000_000)}},
            1,
        ),
        (doc! {"_id": {"$gt": 1}}, 2),
        (doc! {"status": {"$regex": "^pa"}}, 2),
        (doc! {"$or": [{"qty": 1}, {"status": "void"}]}, 2),
        (doc! {"total": Decimal128::from_str("59.9").unwrap()}, 1),
    ] {
        let c = e.count(&mut s, &ns, &f, 0, 0).await.unwrap();
        assert_eq!(c, want, "count {f}");
        let got = all(
            &e,
            &mut s,
            &ns,
            FindSpec {
                filter: f.clone(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(got.len() as u64, want, "find {f}");
    }
    // sort, skip, limit, projection
    let got = all(
        &e,
        &mut s,
        &ns,
        FindSpec {
            filter: doc! {},
            sort: Some(doc! {"qty": -1}),
            skip: 1,
            limit: 2,
            projection: Some(doc! {"qty": 1, "_id": 0}),
        },
    )
    .await;
    assert_eq!(got, vec![doc! {"qty": 3i64}, doc! {"qty": 2.5}]);
    // index: field columns and a MariaDB index; exact equality uses them
    let (b, a, _) = e
        .create_indexes(
            &mut s,
            &ns,
            &[doc! {"key": {"status": 1, "qty": -1}, "name": "st"}],
            false,
        )
        .await
        .unwrap();
    assert_eq!((b, a), (1, 2));
    let m = e.meta(&mut s, &ns).await.unwrap().unwrap();
    let t = crate::translate::translate(&filter::parse(&doc! {"status": "paid"}).unwrap(), &m);
    assert!(t.exact && t.sql.contains("_c` = 40"), "{}", t.sql);
    let plan = s
        .query(&format!(
            "EXPLAIN SELECT _id FROM {db}.orders WHERE {}",
            t.sql
        ))
        .await
        .unwrap();
    let explain: String = plan
        .iter()
        .flatten()
        .flatten()
        .map(|b| String::from_utf8_lossy(b).into_owned() + " ")
        .collect();
    assert!(explain.contains("sprig_dix_1"), "{explain}");
    for (f, want) in [
        (doc! {"status": "paid"}, 2u64),
        (doc! {"qty": 10}, 1),
        (doc! {"status": {"$gt": "new"}}, 3),
    ] {
        assert_eq!(
            e.count(&mut s, &ns, &f, 0, 0).await.unwrap(),
            want,
            "indexed {f}"
        );
    }
    let idx = e.list_indexes(&mut s, &ns).await.unwrap().unwrap();
    assert_eq!(idx[1].get_str("name").unwrap(), "st");
    // unique index over duplicates fails with 11000
    let err = e
        .create_indexes(
            &mut s,
            &ns,
            &[doc! {"key": {"status": 1}, "unique": true}],
            false,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, code::DUPLICATE_KEY);
    // updates
    let r = e.update(&mut s, &ns, &doc! {"_id": oid}, &Bson::Document(doc! {"$set": {"status": "shipped"}, "$inc": {"qty": 1i64}, "$push": {"items": {"sku": "C"}}}), false, false, &[], None).await.unwrap();
    assert_eq!((r.matched, r.modified), (1, 1));
    let got = all(
        &e,
        &mut s,
        &ns,
        FindSpec {
            filter: doc! {"_id": oid},
            ..Default::default()
        },
    )
    .await;
    assert_eq!(got[0].get("qty"), Some(&Bson::Int64(4)));
    assert_eq!(got[0].get("total"), docs[0].get("total"));
    let r = e
        .update(
            &mut s,
            &ns,
            &doc! {"status": "paid"},
            &Bson::Document(doc! {"$set": {"flag": true}}),
            true,
            false,
            &[],
            None,
        )
        .await
        .unwrap();
    assert_eq!((r.matched, r.modified), (1, 1));
    let r = e
        .update(
            &mut s,
            &ns,
            &doc! {"_id": 99},
            &Bson::Document(doc! {"$set": {"a": 1}}),
            false,
            true,
            &[],
            None,
        )
        .await
        .unwrap();
    assert_eq!(r.upserted, Some(Bson::Int32(99)));
    let r = e
        .update(
            &mut s,
            &ns,
            &doc! {"_id": 99},
            &Bson::Document(doc! {"$set": {"a": 1}}),
            false,
            true,
            &[],
            None,
        )
        .await
        .unwrap();
    assert_eq!((r.matched, r.modified), (1, 0));
    // aggregation with a pushed-down $match and $lookup
    let ns2 = Ns {
        db: db.into(),
        coll: "customers".into(),
    };
    e.insert(
        &mut s,
        &ns2,
        vec![
            doc! {"_id": 42, "name": "Acme"},
            doc! {"_id": 7, "name": "Bolt"},
        ],
        true,
    )
    .await
    .unwrap();
    let out = e.aggregate(&mut s, &ns, &[
        doc! {"$match": {"customer.id": {"$exists": true}}},
        doc! {"$group": {"_id": "$customer.id", "n": {"$sum": 1}}},
        doc! {"$lookup": {"from": "customers", "localField": "_id", "foreignField": "_id", "as": "c"}},
        doc! {"$sort": {"_id": 1}},
    ]).await.unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(
        out[1].get_array("c").unwrap()[0]
            .as_document()
            .unwrap()
            .get_str("name")
            .unwrap(),
        "Acme"
    );
    // delete
    let (n, _) = e
        .delete(&mut s, &ns, &doc! {"status": "void"}, false, None)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let (n, _) = e
        .delete(&mut s, &ns, &doc! {"qty": {"$gt": 0}}, true, None)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let cols = e
        .list_collections(&mut s, db, &doc! {}, false)
        .await
        .unwrap();
    assert_eq!(cols.len(), 2);
    e.drop_indexes(&mut s, &ns, &Bson::String("*".into()))
        .await
        .unwrap();
    assert!(e.drop(&mut s, &ns).await.unwrap());
    e.drop_database(&mut s, db).await.unwrap();
}

/// R33: approximate `$vectorSearch` from the side table's HNSW index,
/// validated by recall against the exact search, and kept in step with
/// inserts, updates and deletes. Uses `r5search_vxtest`, dropped at the end.
#[tokio::test]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::items_after_statements
)]
async fn live_vector_ann_recall() {
    let Ok(url) = std::env::var("SPRIG_TEST_DOC_MARIADB") else {
        eprintln!("SPRIG_TEST_DOC_MARIADB not set: skipped");
        return;
    };
    let conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    let mut s = Direct(conn);
    let e = Engine::default();
    let db = "r5search_vxtest";
    let _ = s.exec(&format!("DROP DATABASE IF EXISTS {db}")).await;
    let ns = Ns {
        db: db.into(),
        coll: "items".into(),
    };
    const DIMS: usize = 16;
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % 20_000) as f64 / 10_000.0 - 1.0
    };
    let mut vec_of = |n: usize| -> Vec<Vec<f64>> {
        (0..n).map(|_| (0..DIMS).map(|_| rnd()).collect()).collect()
    };
    let vs = vec_of(3000);
    let docs: Vec<Document> = vs
        .iter()
        .enumerate()
        .map(|(i, v)| doc! {"_id": i as i64, "kind": if i % 2 == 0 { "even" } else { "odd" }, "v": v.clone()})
        .collect();
    for chunk in docs.chunks(500) {
        let (n, errs) = e.insert(&mut s, &ns, chunk.to_vec(), true).await.unwrap();
        assert_eq!((n, errs.len()), (chunk.len() as u64, 0));
    }
    e.create_search_indexes(
        &mut s,
        &ns,
        &[doc! {"name": "vx", "type": "vectorSearch", "definition": {"fields": [
            {"type": "vector", "path": "v", "numDimensions": DIMS as i32, "similarity": "euclidean"},
            {"type": "filter", "path": "kind"}]}}],
    )
    .await
    .unwrap();
    let search_nc = |q: &Vec<f64>, exact: bool, filter: Option<Document>, nc: i32| {
        let mut st = doc! {"index": "vx", "path": "v", "queryVector": q.clone(), "limit": 10};
        if exact {
            st.insert("exact", true);
        } else {
            st.insert("numCandidates", nc);
        }
        if let Some(f) = filter {
            st.insert("filter", f);
        }
        vec![doc! {"$vectorSearch": st}, doc! {"$project": {"_id": 1}}]
    };
    let search =
        |q: &Vec<f64>, exact: bool, filter: Option<Document>| search_nc(q, exact, filter, 100);
    let ids =
        |d: Vec<Document>| -> Vec<i64> { d.iter().map(|x| x.get_i64("_id").unwrap()).collect() };
    let queries = vec_of(40);
    let (mut hit, mut total) = (0usize, 0usize);
    let started = std::time::Instant::now();
    for q in &queries {
        let ex = ids(e
            .aggregate(&mut s, &ns, &search(q, true, None))
            .await
            .unwrap());
        let ap = ids(e
            .aggregate(&mut s, &ns, &search(q, false, None))
            .await
            .unwrap());
        assert_eq!(ap.len(), 10);
        total += ex.len();
        hit += ex.iter().filter(|x| ap.contains(x)).count();
    }
    let recall = hit as f64 / total as f64;
    let mut exact_ids = Vec::new();
    for q in &queries {
        exact_ids.push(ids(e
            .aggregate(&mut s, &ns, &search(q, true, None))
            .await
            .unwrap()));
    }
    let t_ann = std::time::Instant::now();
    let mut hit10 = 0usize;
    for (q, ex) in queries.iter().zip(&exact_ids) {
        let ap = ids(e
            .aggregate(&mut s, &ns, &search_nc(q, false, None, 10))
            .await
            .unwrap());
        hit10 += ex.iter().filter(|x| ap.contains(x)).count();
    }
    let ann_ms = t_ann.elapsed().as_millis() / 40;
    let t_ex = std::time::Instant::now();
    for q in &queries {
        let _ = e
            .aggregate(&mut s, &ns, &search(q, true, None))
            .await
            .unwrap();
    }
    let ex_ms = t_ex.elapsed().as_millis() / 40;
    println!(
        "vector ANN: 3000 x {DIMS} dims, 40 queries: recall@10 {recall:.3} at numCandidates 100, {:.3} at 10; \
         {ann_ms} ms per approximate search, {ex_ms} ms per exact one ({:?} in all)",
        hit10 as f64 / total as f64,
        started.elapsed()
    );
    assert!(recall >= 0.9, "recall@10 {recall}");
    // The side table holds every vector, through the HNSW index.
    let side =
        super::models::vector_side_table(&e.meta(&mut s, &ns).await.unwrap().unwrap().table, "vx");
    let n = s
        .query(&format!("SELECT COUNT(*) FROM {db}.{side}"))
        .await
        .unwrap();
    assert_eq!(n[0][0].as_deref(), Some(&b"3000"[..]));
    // With a filter on a declared field: only matching documents.
    let f = ids(e
        .aggregate(
            &mut s,
            &ns,
            &search(&queries[0], false, Some(doc! {"kind": "odd"})),
        )
        .await
        .unwrap());
    assert_eq!(f.len(), 10);
    assert!(f.iter().all(|x| x % 2 == 1));
    // Writes: a new document is its own nearest; moved and deleted ones
    // follow (after the side table's next sync, at most a second).
    let fresh = vec_of(1).remove(0);
    e.insert(
        &mut s,
        &ns,
        vec![doc! {"_id": 5000_i64, "kind": "even", "v": fresh.clone()}],
        true,
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(
        ids(e
            .aggregate(&mut s, &ns, &search(&fresh, false, None))
            .await
            .unwrap())[0],
        5000
    );
    let moved = vec_of(1).remove(0);
    e.update(
        &mut s,
        &ns,
        &doc! {"_id": 7_i64},
        &Bson::Document(doc! {"$set": {"v": moved.clone()}}),
        false,
        false,
        &[],
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(
        ids(e
            .aggregate(&mut s, &ns, &search(&moved, false, None))
            .await
            .unwrap())[0],
        7
    );
    e.delete(&mut s, &ns, &doc! {"_id": 5000_i64}, true, None)
        .await
        .unwrap();
    assert!(
        !ids(e
            .aggregate(&mut s, &ns, &search(&fresh, false, None))
            .await
            .unwrap())
        .contains(&5000)
    );
    // Dropping the index drops its side table.
    e.drop_search_index(&mut s, &ns, "vx").await.unwrap();
    let left = s
        .query(&format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{side}'"
        ))
        .await
        .unwrap();
    assert_eq!(left[0][0].as_deref(), Some(&b"0"[..]));
    let _ = s.exec(&format!("DROP DATABASE IF EXISTS {db}")).await;
}
