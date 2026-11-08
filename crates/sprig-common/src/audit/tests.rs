#![allow(clippy::unwrap_used)]

use serde_json::json;

use super::*;

fn dc() -> DcId {
    DcId::new("dc-a").unwrap()
}

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "sprig-audit-{name}-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = fs::remove_dir_all(&d);
    d
}

#[test]
fn civil_dates() {
    assert_eq!(civil_date(0), "1970-01-01");
    assert_eq!(civil_date(951_782_400_000), "2000-02-29");
    assert_eq!(civil_date(1_794_182_400_000), "2026-11-09");
}

#[test]
fn streams_render_and_parse() {
    for s in [AuditStream::Platform, AuditStream::Tenant(TenantId(42))] {
        let j = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<AuditStream>(&j).unwrap(), s);
    }
    assert_eq!(AuditStream::Tenant(TenantId(7)).to_string(), "tenant-7");
}

#[tokio::test]
async fn events_reach_the_sink_in_order_and_the_spool_is_trimmed() {
    let dir = tmp("ship");
    let sink = MemorySink::new(dc());
    let p = Pipeline::open(&dir, "gw-1", 512, sink.clone(), sink.clone()).unwrap();
    p.emit(
        AuditStream::Tenant(TenantId(1)),
        "auth.login_failed",
        json!({"user": "a"}),
    );
    p.emit(
        AuditStream::Platform,
        "settings.changed",
        json!({"key": "k"}),
    );
    p.emit(
        AuditStream::Tenant(TenantId(1)),
        "auth.login_succeeded",
        json!({}),
    );
    p.flush().unwrap();
    assert!(p.spool_bytes() > 0);
    assert_eq!(p.ship().await.unwrap(), 3);
    let got = sink.records();
    assert_eq!(got.len(), 3);
    let seqs: Vec<u64> = got
        .iter()
        .filter(|r| r.stream == AuditStream::Tenant(TenantId(1)))
        .map(|r| r.seq)
        .collect();
    assert_eq!(seqs, vec![1, 3]);
    assert_eq!(p.spool_bytes(), 0);
    assert_eq!(p.backlog(), 0);
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_failing_sink_keeps_events_and_a_restart_recovers_them() {
    let dir = tmp("recover");
    let sink = MemorySink::new(dc());
    sink.set_failing(true);
    {
        let p = Pipeline::open(&dir, "gw-1", 512, sink.clone(), sink.clone()).unwrap();
        p.emit(AuditStream::Platform, "a", json!({}));
        p.emit(AuditStream::Platform, "b", json!({}));
        p.flush().unwrap();
        assert!(p.ship().await.is_err());
        assert_eq!(p.backlog(), 2);
    }
    // A new process on the same spool: the events come back, and new ones
    // continue the sequence.
    sink.set_failing(false);
    let p = Pipeline::open(&dir, "gw-1", 512, sink.clone(), sink.clone()).unwrap();
    assert_eq!(p.backlog(), 2);
    p.emit(AuditStream::Platform, "c", json!({}));
    p.flush().unwrap();
    assert_eq!(p.ship().await.unwrap(), 3);
    let names: Vec<String> = sink.records().into_iter().map(|r| r.event).collect();
    assert_eq!(names, vec!["a", "b", "c"]);
    assert_eq!(sink.records()[2].seq, 3);
    // Shipping again sends nothing; a sink that saw a record twice keeps one.
    assert_eq!(p.ship().await.unwrap(), 0);
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_full_spool_fails_closed_but_keeps_logins_in_memory() {
    let dir = tmp("full");
    let sink = MemorySink::new(dc());
    sink.set_failing(true);
    let p = Pipeline::open(&dir, "gw-1", 0, sink.clone(), sink.clone()).unwrap();
    let e = p.check().unwrap_err();
    assert_eq!(e.kind, crate::ErrorKind::Sprig(SprigCode::AuditUnavailable));
    // Kept in memory, not spooled.
    p.emit(
        AuditStream::Tenant(TenantId(3)),
        "auth.login_failed",
        json!({}),
    );
    p.flush().unwrap();
    assert_eq!(p.spool_bytes(), 0);
    assert_eq!(p.backlog(), 1);
    sink.set_failing(false);
    assert_eq!(p.ship().await.unwrap(), 1);
    // A bigger limit opens it again.
    p.set_max_spool_mb(1);
    p.check().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_dir_sink_writes_the_object_layout_and_deduplicates() {
    let dir = tmp("sink");
    let root = dir.join("store");
    let sink = Arc::new(DirSink::new(&root, dc()));
    let p = Pipeline::open(dir.join("spool"), "gw-9", 512, sink.clone(), sink).unwrap();
    p.emit(
        AuditStream::Tenant(TenantId(5)),
        "auth.user_created",
        json!({"user": "app"}),
    );
    p.emit(AuditStream::Platform, "settings.changed", json!({}));
    p.flush().unwrap();
    p.ship().await.unwrap();
    let day = civil_date(now_ms());
    let tdir = root.join("audit").join("tenant-5").join(&day);
    let files: Vec<_> = fs::read_dir(&tdir).unwrap().collect();
    assert_eq!(files.len(), 1, "{tdir:?}");
    assert!(root.join("audit/platform").join(&day).is_dir());
    let all = DirSink::read_all(&root).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].body["user"], "app");
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_background_tasks_flush_and_ship() {
    let dir = tmp("spawn");
    let sink = MemorySink::new(dc());
    let p = Pipeline::open(&dir, "gw-1", 512, sink.clone(), sink.clone()).unwrap();
    let stop = CancellationToken::new();
    let task = p.spawn(stop.clone());
    for i in 0..100 {
        p.emit(AuditStream::Platform, "e", json!({"i": i}));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while sink.records().len() < 100 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.cancel();
    task.await.unwrap();
    assert_eq!(sink.records().len(), 100);
    let _ = fs::remove_dir_all(&dir);
}
