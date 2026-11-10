//! Live: `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`). Works in the schema
//! `approvals_live_<pid>`, created bare (no catalog tables) so the store
//! migrates the module on first use, and dropped afterwards.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_catalog::{CatalogEndpoint, StaticLocator, Store};
use sprig_common::ids::{DbId, ObjectId, TenantId};

use super::*;
use crate::machine::{Act, TickChange};
use crate::model::{Action, Kind, Level, Mode, Subject};
use crate::store::Filter;

fn p(cols: &str, score: f64) -> Proposal {
    Proposal {
        kind: Kind::IndexCreate,
        subject: Subject {
            tenant: Some(TenantId(7)),
            db: Some(DbId(1)),
            object: Some(ObjectId(3)),
            ..Subject::default()
        },
        target: "t7/o3".into(),
        action: Action {
            text: format!("ALTER TABLE shop.orders ADD INDEX ({cols})"),
            machine: serde_json::json!({ "cols": cols }),
        },
        evidence: serde_json::json!({ "gain_ms_h": score }),
        score,
        summary: "an index".into(),
        emergency: false,
    }
}

#[tokio::test]
async fn live_catalog_store_round_trip() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
        .pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        );
    let pool = mysql_async::Pool::new(opts);
    let schema = format!("approvals_live_{}", std::process::id());
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    conn.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    // The migrator records formats in the core `settings` table.
    Migrator::new(&[])
        .unwrap()
        .run(&mut conn, &schema, &MigrateOpts::default())
        .await
        .unwrap();
    let locator = StaticLocator::new().with(
        TenantId(7),
        CatalogEndpoint {
            pool: pool.clone(),
            store: Store::new(schema.clone()),
        },
    );
    let store = Arc::new(CatalogRecStore::new(Arc::new(locator), TenantId(7)));
    let r = async move {
        let mut pol = Policy::fixed(Mode::Approve, Level::Table);
        pol.escalate_after = Some(Duration::from_secs(60));
        // First raise migrates the module (the table is missing).
        let a = store
            .raise(&p("a", 10.0), &pol, &[TenantId(1)], 1_000)
            .await
            .unwrap();
        let Verdict::Wait(id) = a.verdict.clone() else {
            panic!("{a:?}")
        };
        assert_eq!(id, RecId::Tenant(TenantId(7), 1));
        // Refresh: the same row.
        let b = store
            .raise(&p("a", 12.0), &pol, &[TenantId(1)], 2_000)
            .await
            .unwrap();
        assert_eq!(b.verdict, Verdict::Wait(id.clone()));
        let got = store.get(1).await.unwrap().unwrap();
        assert_eq!(got.refreshes, 1);
        assert_eq!(got.id, id);
        // Concurrent raises of new advice for another target: one row.
        let q = |n: u32| {
            let mut x = p("z", 5.0);
            x.target = "t7/o9".into();
            let store = &*store;
            let pol = &pol;
            async move { store.raise(&x, pol, &[], 3_000 + i64::from(n)).await }
        };
        let (r1, r2, r3) = tokio::join!(q(1), q(2), q(3));
        for r in [r1, r2, r3] {
            assert!(matches!(r.unwrap().verdict, Verdict::Wait(_)));
        }
        let live = store.list(&Filter::default()).await.unwrap();
        assert_eq!(live.len(), 2, "{live:?}");
        // Ignore, suppression, approval by version, the due index.
        let (ign, _) = crate::store::modify(&*store, &id, |r| {
            crate::machine::act(
                r,
                &Act::Ignore {
                    reason: "no".into(),
                    for_days: Some(2),
                    forever: false,
                },
                "ana",
                1,
                2.0,
                5_000,
            )
            .map(Some)
        })
        .await
        .unwrap();
        assert_eq!(ign.state, State::Ignored);
        let s = store.raise(&p("a", 15.0), &pol, &[], 6_000).await.unwrap();
        assert_eq!(s.verdict, Verdict::Suppressed(id.clone()));
        let s = store.raise(&p("a", 25.0), &pol, &[], 7_000).await.unwrap();
        let Verdict::Wait(id2) = s.verdict else {
            panic!()
        };
        assert_ne!(id2, id);
        let due = store.due(7_000 + 61_000_000, 10).await.unwrap();
        assert!(due.iter().any(|r| r.id == id2), "{due:?}");
        let mut r = due.into_iter().find(|r| r.id == id2).unwrap();
        let v = r.version;
        assert!(matches!(
            crate::machine::tick(&mut r, 7_000 + 61_000_000, Duration::from_secs(3600)),
            Some(TickChange::Escalated(_))
        ));
        assert!(store.put(&r, v).await.unwrap());
        assert!(!store.put(&r, v).await.unwrap(), "stale version refused");
        let f = Filter {
            states: vec![State::Ignored],
            ..Filter::default()
        };
        assert_eq!(store.list(&f).await.unwrap().len(), 1);
        let f = Filter {
            kinds: vec!["index".into()],
            all: true,
            ..Filter::default()
        };
        assert_eq!(store.list(&f).await.unwrap().len(), 3);
        // Subscriptions.
        let (mut c, recs) = store.open().await.unwrap();
        recs.set_subscription(
            &mut c,
            &Subscription {
                user: "ana".into(),
                email: "ana@example.com".into(),
                delivery: "immediate".into(),
                kinds: "*".into(),
                last_digest_us: None,
            },
            1,
        )
        .await
        .unwrap();
        assert_eq!(recs.subscriptions(&mut c).await.unwrap().len(), 1);
    };
    let out = tokio::spawn(r).await;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
    out.unwrap();
}
