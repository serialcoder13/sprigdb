#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::cast_precision_loss)]

use sprig_common::ids::DcId;
use sprig_settings::{Level, Registry, Value};

use super::*;

const GIB: u64 = 1 << 30;

fn inputs(tables: u64, used: u64) -> TuningInputs {
    TuningInputs {
        tables,
        routines: 0,
        tables_used: used,
        peak_threads_running: 10,
        max_connections: 500,
        max_routines_per_schema: 2,
        memory_bytes: Some(64 * GIB),
    }
}

fn defaults() -> BTreeMap<ServerVar, u64> {
    BTreeMap::from([
        (ServerVar::TableDefinitionCache, 400),
        (ServerVar::TableOpenCache, 2000),
        (ServerVar::InnodbOpenFiles, 2000),
        (ServerVar::OpenFilesLimit, 1_048_576),
        (ServerVar::StoredProgramCache, 256),
    ])
}

#[test]
fn want_follows_the_formulas() {
    let w = want(&inputs(50_000, 20_000), &Costs::default(), 10);
    assert_eq!(w.def, 55_400);
    assert_eq!(w.toc, 24_080);
    assert_eq!(w.iof, 24_080);
    assert_eq!(w.ofl, 24_080 + 24_080 + 1000 + 1000);
    assert_eq!(w.spc, 256);
    assert!(!w.over_budget);
    // Floors and the routine cache's power of two.
    let mut i = inputs(10, 10);
    i.max_routines_per_schema = 300;
    let w = want(&i, &Costs::default(), 10);
    assert_eq!((w.def, w.toc, w.spc), (411, 2000, 1024));
}

#[test]
fn the_budget_shrinks_spc_then_toc_then_def() {
    let mut i = inputs(100_000, 30_000);
    i.max_routines_per_schema = 2000;
    // 1 GiB at 10%: 107 MB for caches.
    i.memory_bytes = Some(GIB);
    let w = want(&i, &Costs::default(), 10);
    assert_eq!(w.spc, 256);
    assert_eq!(w.toc, 30_000, "toc stops at Ta");
    assert_eq!(w.def, 100_000, "def stops at T");
    assert!(w.over_budget);
    // A budget that holds the definitions but not every open instance.
    i.memory_bytes = Some(20 * GIB);
    let w = want(&i, &Costs::default(), 10);
    assert!(!w.over_budget);
    assert!(w.spc < 4096);
    assert!(w.mem_bytes <= 20.0 * GIB as f64 * 0.1);
    // Unknown memory: no budget.
    i.memory_bytes = None;
    assert!(!want(&i, &Costs::default(), 10).over_budget);
}

#[test]
fn plan_grows_at_once_shrinks_after_a_day_and_respects_the_margin() {
    let mut t = Tuner::new();
    let s = TuningSettings::default();
    let k = Costs::default();
    let p = t.plan(&inputs(10_000, 10_000), &defaults(), &k, &s, true, 1000.0, 0);
    let got: Vec<(ServerVar, u64, ApplyMode)> =
        p.wanted.iter().map(|c| (c.var, c.to, c.mode)).collect();
    assert_eq!(
        got,
        vec![
            (ServerVar::TableDefinitionCache, 11_400, ApplyMode::Live),
            (ServerVar::TableOpenCache, 12_080, ApplyMode::Live),
            (ServerVar::InnodbOpenFiles, 12_080, ApplyMode::NextRestart),
        ]
    );
    assert_eq!(
        p.statements(),
        vec![
            "SET GLOBAL table_definition_cache = 11400;".to_owned(),
            "SET GLOBAL table_open_cache = 12080;".to_owned(),
            "-- innodb_open_files = 12080 in the server configuration (read-only; next restart)"
                .to_owned(),
        ]
    );
    t.applied(&p, 1000.0, 0);
    let now_cur = BTreeMap::from([
        (ServerVar::TableDefinitionCache, 11_400),
        (ServerVar::TableOpenCache, 12_080),
        (ServerVar::InnodbOpenFiles, 2000),
        (ServerVar::OpenFilesLimit, 1_048_576),
        (ServerVar::StoredProgramCache, 256),
    ]);
    // 10% more tables: under tuning.min_change_pct, nothing.
    let p = t.plan(&inputs(11_000, 11_000), &now_cur, &k, &s, true, 0.0, 600);
    assert!(p.wanted.iter().all(|c| c.var == ServerVar::InnodbOpenFiles), "{p:?}");
    // Fewer tables: the shrink waits 24 h.
    let p = t.plan(&inputs(5000, 5000), &now_cur, &k, &s, true, 0.0, 3600);
    assert!(p.wanted.iter().all(|c| c.var == ServerVar::InnodbOpenFiles), "{p:?}");
    assert!(p.held.iter().any(|h| h.contains("after 24 h")));
    let p = t.plan(&inputs(5000, 5000), &now_cur, &k, &s, true, 0.0, 3600 + 86_400);
    assert!(p.wanted.iter().any(|c| c.var == ServerVar::TableDefinitionCache && c.to == 5900));
}

#[test]
fn table_open_cache_changes_rarely_and_the_ratchet_doubles_the_margin() {
    let mut t = Tuner::new();
    let s = TuningSettings::default();
    let k = Costs::default();
    let mut cur = defaults();
    let p = t.plan(&inputs(10_000, 10_000), &cur, &k, &s, true, 1000.0, 0);
    t.applied(&p, 1000.0, 0);
    cur.insert(ServerVar::TableOpenCache, 12_080);
    cur.insert(ServerVar::TableDefinitionCache, 11_400);
    // 30% more within the interval: held.
    let p = t.plan(&inputs(13_000, 13_000), &cur, &k, &s, true, 900.0, 600);
    assert!(!p.wanted.iter().any(|c| c.var == ServerVar::TableOpenCache));
    assert!(p.held.iter().any(|h| h.contains("interval")));
    // An hour on, misses haven't fallen: the margin doubles to 40%, so a
    // 30% growth is not enough.
    let p = t.plan(&inputs(13_000, 13_000), &cur, &k, &s, true, 900.0, 3600);
    assert_eq!(t.margin_mult(), 2);
    assert!(!p.wanted.iter().any(|c| c.var == ServerVar::TableOpenCache));
    let p = t.plan(&inputs(15_000, 15_000), &cur, &k, &s, true, 900.0, 3700);
    assert!(p.wanted.iter().any(|c| c.var == ServerVar::TableOpenCache));
}

#[test]
fn external_clusters_and_modes() {
    let k = Costs::default();
    let mut s = TuningSettings::default();
    // External (not managed): recommendations.
    let p = Tuner::new().plan(&inputs(10_000, 10_000), &defaults(), &k, &s, false, 0.0, 0);
    assert!(p.wanted.iter().all(|c| c.mode == ApplyMode::RecommendOnly));
    // Unknown memory: recommendations even when managed.
    let mut i = inputs(10_000, 10_000);
    i.memory_bytes = None;
    let p = Tuner::new().plan(&i, &defaults(), &k, &s, true, 0.0, 0);
    assert!(p.wanted.iter().all(|c| c.mode == ApplyMode::RecommendOnly));
    s.mode = TuningMode::Off;
    assert!(Tuner::new().plan(&i, &defaults(), &k, &s, true, 0.0, 0).wanted.is_empty());
}

#[test]
fn table_open_cache_is_capped_by_the_running_descriptors() {
    let mut cur = defaults();
    cur.insert(ServerVar::OpenFilesLimit, 32_198);
    let p = Tuner::new().plan(
        &inputs(40_000, 40_000),
        &cur,
        &Costs::default(),
        &TuningSettings::default(),
        true,
        0.0,
        0,
    );
    let toc = p.wanted.iter().find(|c| c.var == ServerVar::TableOpenCache).unwrap();
    assert_eq!(toc.to, 32_198 - 2000 - 1000 - 1000);
    let ofl = p.wanted.iter().find(|c| c.var == ServerVar::OpenFilesLimit).unwrap();
    assert_eq!(ofl.mode, ApplyMode::NextRestart);
}

#[test]
fn keys_are_registered() {
    let reg = Registry::global().unwrap();
    let mut s = MemSettings::new(reg);
    let dc = DcId::new("mum1").unwrap();
    let c = ClusterId::new("mum1-c1").unwrap();
    let ctx = SettingCtx {
        cluster: Some(&c),
        ..SettingCtx::process(&dc)
    };
    assert_eq!(TuningSettings::resolve(&s, &ctx), TuningSettings::default());
    s.set_value(Level::Cluster, &ctx, "tuning.table_open_cache_mode", &Value::Str("recommend".into()))
        .unwrap();
    s.set_value(Level::Cluster, &ctx, "tuning.memory_budget_pct", &Value::Int(30)).unwrap();
    let r = TuningSettings::resolve(&s, &ctx);
    assert_eq!((r.mode, r.memory_budget_pct), (TuningMode::Recommend, 30));
}
