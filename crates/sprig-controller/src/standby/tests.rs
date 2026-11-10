//! The standby loop's records. The loop itself runs live in
//! `tests/live_standby_bins.rs` (two DCs, real binaries).

#![allow(clippy::unwrap_used)]

use serde_json::json;

use super::*;

#[test]
fn job_standbys_round_trip_through_the_directory_entry() {
    let sb = JobStandby {
        cluster: ClusterId::new("ams1-c1").unwrap(),
        source: ClusterId::new("fra1-c1").unwrap(),
        job: JobId::new_v7(),
        created_ms: 7,
    };
    let mut raw = serde_json::Map::new();
    raw.insert(
        FIELD.into(),
        json!({"ams1": serde_json::to_value(&sb).unwrap(), "bad dc!": {"x": 1}, "wit1": 3}),
    );
    raw.insert("standby_dcs".into(), json!(["ams1"]));
    let got = job_standbys(&raw);
    assert_eq!(got.len(), 1);
    assert_eq!(got[&DcId::new("ams1").unwrap()], sb);
    assert_eq!(standby_dcs(&raw), ["ams1"]);
    assert!(job_standbys(&serde_json::Map::new()).is_empty());
}

/// RL3 (r2harden): a tenant's own `failover.mode` and
/// `failover.rpo_seconds` reach its directory entry, where the global
/// failover decision reads them when the home DC (and so the catalog) is
/// lost; the copy goes away when the tenant's rows no longer differ.
#[tokio::test]
async fn tenant_failover_settings_are_copied_to_the_directory() {
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_meta::MemStore;
    use sprig_settings::{Registry, SettingsStores};

    use crate::tenant_scopes::{TenantScopes, build_rows};

    let dc = DcId::new("fra1").unwrap();
    let dcs = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(dcs.with_tier(Tier::Global));
    let store: Arc<dyn MetaStore> = Arc::new(dcs.clone());
    let live = LiveSettings::start(
        Registry::global().unwrap(),
        SettingsStores::new(Arc::clone(&global), Arc::clone(&store)).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let scopes = TenantScopes::new(
        Arc::clone(&global),
        Arc::clone(&store),
        Arc::new(crate::catalogs::StaticPools::new()),
    )
    .unwrap();
    let t = TenantId(41);
    let entry = json!({"v": 1, "home_dc": "fra1", "epoch": 1, "state": "active",
        "catalog_version": 2, "failover": {"failover_id": "fo-1"}});
    global
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key: home::directory_key(t).unwrap(),
                value: serde_json::to_vec(&entry).unwrap(),
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
    let row = |key: &str, v: Value| SettingRow {
        scope: SettingScope::Tenant,
        key: key.into(),
        value: Some(v),
        bounds: None,
        set_by: "op:test".into(),
        set_at: std::time::SystemTime::UNIX_EPOCH,
        version: 2,
    };
    let seed = |rows: Vec<SettingRow>| {
        scopes.seed(
            t,
            2,
            build_rows(Registry::global().unwrap(), t, 2, rows),
        );
    };
    seed(vec![
        row("failover.rpo_seconds", json!(1)),
        row("failover.mode", json!("manual")),
    ]);
    let l = StandbyLoop::new(
        dc.clone(),
        Arc::clone(&store),
        Arc::clone(&global),
        Arc::new(sprig_common::time::SystemClock),
        Some(live),
    )
    .with_tenant_scopes(Arc::clone(&scopes));
    let e = home::read_home(&*global, t).await.unwrap().unwrap();
    l.copy_failover_settings(t, &e).await.unwrap();
    let e = home::read_home(&*global, t).await.unwrap().unwrap();
    assert_eq!(
        e.raw["failover"],
        json!({"failover_id": "fo-1", "mode": "manual", "rpo_seconds": 1}),
        "the tenant's own values, the promotion's fields kept"
    );
    // Its rows now say what the DC says: the copy goes.
    seed(Vec::new());
    l.copy_failover_settings(t, &e).await.unwrap();
    let e = home::read_home(&*global, t).await.unwrap().unwrap();
    assert_eq!(e.raw["failover"], json!({"failover_id": "fo-1"}));
}
