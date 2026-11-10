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
