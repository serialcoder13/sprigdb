#![allow(clippy::unwrap_used)]

use sprig_catalog::Migrator;
use sprig_catalog::migrate::MigrateOpts;

use super::*;

fn decision(at_us: i64, subject: &str) -> NewDecision {
    NewDecision {
        decided_at_us: at_us,
        component: "balancer".to_owned(),
        kind: "shard.move".to_owned(),
        subject_type: "shard".to_owned(),
        subject_id: subject.to_owned(),
        mode: Mode::Applied,
        outcome: Outcome::Scheduled,
        reason_code: "cluster_hot".to_owned(),
        summary: "x".repeat(600),
        evidence: serde_json::json!({"gain": 0.28, "from": "mum1-c7"}),
        alternatives: Some(serde_json::json!([{"cluster": "mum1-c9", "full_after": 0.5}])),
        job_id: None,
        parent_id: None,
        actor: "ctl-1".to_owned(),
        dc_id: "mum1".to_owned(),
    }
}

#[test]
fn enums_round_trip() {
    for m in [
        Mode::Applied,
        Mode::Recommended,
        Mode::DryRun,
        Mode::Operator,
    ] {
        assert_eq!(Mode::parse(m.as_str()).unwrap(), m);
    }
    assert!(Outcome::parse("bogus").is_err());
    assert!(Outcome::Succeeded.is_final());
    assert!(!Outcome::Running.is_final());
    assert_eq!(DECISIONS_MODULE.newest(), 1);
}

#[tokio::test]
async fn mem_log_records_updates_and_purges() {
    let log = MemDecisionLog::new();
    let t = TenantId(42);
    let a = log
        .record(t, &decision(1_000_000, "t42/g1/s1"))
        .await
        .unwrap();
    let b = log
        .record(t, &decision(9_000_000, "t42/g1/s2"))
        .await
        .unwrap();
    assert_ne!(a, b);
    log.update_outcome(t, a, Outcome::Succeeded, 2_000_000)
        .await
        .unwrap();
    assert_eq!(log.of(t)[0].d.outcome, Outcome::Succeeded);
    assert_eq!(log.of(t)[0].updated_at_us, 2_000_000);
    assert_eq!(log.purge_before(5_000_000), 1);
    assert_eq!(log.of(t).len(), 1);
}

/// Live: `SPRIG_TEST_MARIADB_SINGLE`; migrates the module into
/// `placement_decisions_<pid>` and drops it afterwards.
#[tokio::test]
async fn live_store_round_trip_and_retention() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    let schema = format!("placement_decisions_{}", std::process::id());
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    conn.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    conn.query_drop(format!("USE {schema}")).await.unwrap();
    let r = async {
        Migrator::new(&[DECISIONS_MODULE])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        let st = DecisionStore::new(schema.clone());
        let old = st
            .insert(&mut conn, &decision(1_000_000, "t42/g1/s1"))
            .await
            .unwrap();
        let mut d = decision(1_700_000_000_000_000, "t42/g1/s1");
        d.job_id = Some([7; 16]);
        d.parent_id = Some(old);
        let new = st.insert(&mut conn, &d).await.unwrap();
        st.set_outcome(
            &mut conn,
            new,
            Outcome::Succeeded,
            None,
            1_700_000_000_500_000,
        )
        .await
        .unwrap();
        let got = st.get(&mut conn, new).await.unwrap().unwrap();
        assert_eq!(got.d.outcome, Outcome::Succeeded);
        assert_eq!(got.d.job_id, Some([7; 16]));
        assert_eq!(got.d.parent_id, Some(old));
        assert_eq!(got.d.summary.chars().count(), 512);
        assert_eq!(got.d.evidence["gain"], 0.28);
        assert_eq!(got.d.decided_at_us, 1_700_000_000_000_000);
        assert_eq!(got.updated_at_us, 1_700_000_000_500_000);
        let by = st
            .by_subject(&mut conn, "shard", "t42/g1/s1", 10)
            .await
            .unwrap();
        assert_eq!(
            by.iter().map(|r| r.decision_id).collect::<Vec<_>>(),
            vec![new, old]
        );
        let recent = st
            .recent(&mut conn, "balancer", 1_600_000_000_000_000, 10)
            .await
            .unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(
            st.purge_before(&mut conn, 1_600_000_000_000_000)
                .await
                .unwrap(),
            1
        );
        assert!(st.get(&mut conn, old).await.unwrap().is_none());
    };
    r.await;
    conn.query_drop(format!("DROP DATABASE {schema}"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
}
