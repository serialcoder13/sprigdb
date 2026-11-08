#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;

fn with_tracer<R>(tracer: &Arc<Tracer>, f: impl FnOnce() -> R) -> R {
    let layer = TraceLayer::new(Arc::clone(tracer))
        .with_filter(tracing_subscriber::filter::filter_fn(|m| m.target() == TARGET));
    let sub = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(sub, f)
}

fn statement(error: bool) {
    let root = tracing::info_span!(target: TARGET, "gateway.request", tenant = 42u64, protocol = "mysql", error = tracing::field::Empty);
    let _g = root.enter();
    {
        let b = tracing::info_span!(target: TARGET, "backend.query", cluster = "dc-a-c1");
        let _b = b.enter();
        // Not traced: another target.
        let other = tracing::info_span!("sprig_gateway::other");
        let _o = other.enter();
        assert!(current().is_some());
    }
    if error {
        root.record("error", true);
    }
}

#[test]
fn traceparent_round_trips_and_rejects_bad_input() {
    let t = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let c = TraceContext::parse(t).unwrap();
    assert!(c.sampled);
    assert_eq!(c.traceparent(), t);
    for bad in [
        "",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x",
    ] {
        assert!(TraceContext::parse(bad).is_none(), "{bad}");
    }
}

#[test]
fn a_sampled_statement_shows_gateway_then_backend() {
    let tracer = Arc::new(Tracer::new(1.0, 10_000));
    with_tracer(&tracer, || statement(false));
    let traces = tracer.recent();
    assert_eq!(traces.len(), 1);
    let t = &traces[0];
    assert_eq!(t.kept_because, KeptBecause::Sampled);
    assert_eq!(t.spans.len(), 2, "{t:?}");
    let root = t.root().unwrap();
    assert_eq!(root.name, "gateway.request");
    assert!(root.parent_id.is_none());
    let kids: Vec<_> = t.children_of(root.span_id).collect();
    assert_eq!(kids.len(), 1);
    assert_eq!(kids[0].name, "backend.query");
    assert!(kids[0].duration_nanos <= root.duration_nanos);
    assert!(
        root.attributes
            .contains(&("tenant".to_owned(), "42".to_owned()))
    );
    assert!(tracer.find("protocol", "mysql").is_some());
}

#[test]
fn tail_sampling_keeps_failures_and_slow_roots_only() {
    let tracer = Arc::new(Tracer::new(0.0, 10_000));
    with_tracer(&tracer, || statement(false));
    assert!(tracer.recent().is_empty());
    with_tracer(&tracer, || statement(true));
    let t = tracer.recent();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].kept_because, KeptBecause::Error);
    assert!(t[0].root().unwrap().error);

    tracer.set_policy(0.0, 0);
    with_tracer(&tracer, || statement(false));
    assert_eq!(tracer.recent().last().unwrap().kept_because, KeptBecause::Slow);
}

#[test]
fn an_incoming_traceparent_continues_the_trace() {
    let tracer = Arc::new(Tracer::new(0.0, 10_000));
    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let inside = with_tracer(&tracer, || {
        let root = tracing::info_span!(target: TARGET, "gateway.request", traceparent = tp);
        let _g = root.enter();
        current().unwrap()
    });
    assert_eq!(inside.trace_id, 0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736);
    assert!(inside.sampled);
    let t = tracer.recent();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].root().unwrap().parent_id, Some(0x00f0_67aa_0ba9_02b7));
    // The traceparent itself isn't an attribute.
    assert!(
        t[0].root()
            .unwrap()
            .attributes
            .iter()
            .all(|(k, _)| k != "traceparent")
    );
}

#[test]
fn outside_a_trace_there_is_no_context() {
    let tracer = Arc::new(Tracer::new(1.0, 0));
    with_tracer(&tracer, || {
        let s = tracing::info_span!("not.traced");
        let _g = s.enter();
        assert!(current().is_none());
    });
    assert!(tracer.recent().is_empty());
}

#[test]
fn otlp_json_has_ids_parents_and_status() {
    let tracer = Arc::new(Tracer::new(1.0, 10_000));
    with_tracer(&tracer, || statement(true));
    let t = tracer.recent();
    let v = otlp::encode(&t, &[("service.name".into(), "sprigdb-gateway".into())]);
    let spans = v["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    assert_eq!(spans.len(), 2);
    let root = &spans[1];
    assert_eq!(root["name"], "gateway.request");
    assert_eq!(root["kind"], 2);
    assert_eq!(root["status"]["code"], 2);
    assert_eq!(spans[0]["parentSpanId"], root["spanId"]);
    assert_eq!(root["traceId"].as_str().unwrap().len(), 32);
}

#[tokio::test]
async fn the_exporter_posts_to_the_collector() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let got = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = s.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf);
            if let Some((head, body)) = text.split_once("\r\n\r\n") {
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                if body.len() >= len {
                    s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}")
                        .await
                        .unwrap();
                    return (head.lines().next().unwrap().to_owned(), body.to_owned());
                }
            }
        }
    });
    let tracer = Arc::new(Tracer::new(1.0, 10_000));
    with_tracer(&tracer, || statement(false));
    let stop = CancellationToken::new();
    let e = otlp::OtlpExporter::spawn(Some(format!("http://{addr}")), vec![], stop.clone());
    e.export(&tracer.recent()[0]);
    let (line, body) = tokio::time::timeout(Duration::from_secs(10), got)
        .await
        .unwrap()
        .unwrap();
    stop.cancel();
    assert_eq!(line, "POST /v1/traces HTTP/1.1");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

use tokio_util::sync::CancellationToken;
