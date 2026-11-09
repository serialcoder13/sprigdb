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
