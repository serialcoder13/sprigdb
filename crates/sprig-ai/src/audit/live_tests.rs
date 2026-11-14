//! Live: `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`). Works in the schema
//! `ai_core_live_<pid>`, created bare so the store migrates the module on
//! first use, and dropped afterwards.

use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde_json::json;

use super::*;

#[tokio::test]
async fn live_catalog_audit_round_trip() {
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
    let schema = format!("ai_core_live_{}", std::process::id());
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
    let store = CatalogAudit::new(pool.clone(), schema.clone());
    let r = async {
        let alice = UserId(11);
        let bob = UserId(12);
        assert!(store.create(alice, Role::Admin, "x").await.is_err());
        let c = store
            .create(alice, Role::Dba, "  Why is it slow?  ")
            .await?;
        let other = store.create(bob, Role::Data, "").await?;
        assert_eq!(store.get(alice, c).await?.unwrap().title, "Why is it slow?");
        assert!(store.get(bob, c).await?.is_none(), "only the owner sees it");
        assert_eq!(store.get(bob, other).await?.unwrap().title, "Untitled");
        store
            .append(c, MsgKind::User, &json!({"text": "hi"}), None)
            .await?;
        let hash = [7u8; 32];
        store
            .append(
                c,
                MsgKind::SystemNote,
                &json!({"sha256": "07", "payload": "{}"}),
                Some(hash),
            )
            .await?;
        let msgs = store.messages(c).await?;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].kind, MsgKind::User);
        assert_eq!(msgs[1].sent_hash, Some(hash));
        assert_eq!(msgs[1].body["payload"], "{}");
        assert_eq!(store.list(alice, 10).await?.len(), 1);
        let u = Usage {
            input_tokens: 100,
            output_tokens: 30,
            cache_read_tokens: 900,
            cache_write_tokens: 50,
        };
        store.add_usage(alice, Role::Dba, &u).await?;
        store.add_usage(alice, Role::Dba, &u).await?;
        store.add_usage(bob, Role::Data, &u).await?;
        let today = store.usage_today(alice).await?;
        assert_eq!(today.user, 360);
        assert_eq!(today.tenant, 540);
        // Nothing is old enough to purge; a 0-day window takes the payload.
        assert_eq!(store.purge_payloads(30).await?, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(store.purge_payloads(0).await?, 1);
        let msgs = store.messages(c).await?;
        assert!(msgs[1].body.get("payload").is_none());
        assert_eq!(msgs[1].body["payload_purged"], true);
        assert_eq!(msgs[1].sent_hash, Some(hash));
        Ok::<(), SprigError>(())
    }
    .await;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
    r.unwrap();
}
