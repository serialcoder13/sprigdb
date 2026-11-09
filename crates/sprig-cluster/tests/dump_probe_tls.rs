//! The dump probe and the derived `feed_source` / `mover_source` on a
//! node whose `sprig_repl` requires TLS (r2opfix). Before the binlog
//! reader had TLS, such clusters (every Kubernetes cluster) reported both
//! false.
//!
//! The same variables as `sprig-binlog`'s `live_tls`:
//! `SPRIG_TEST_BINLOG_TLS` (the `REQUIRE SSL` replication account),
//! `SPRIG_TEST_BINLOG_TLS_ROOT` (root, for the control connection) and
//! `SPRIG_TEST_BINLOG_TLS_CA`. Skips when unset.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::option_option, missing_docs)]

use std::path::PathBuf;
use std::time::Duration;

use mysql_async::{Opts, OptsBuilder, SslOpts};
use sprig_cluster::NodeEndpoint;
use sprig_cluster::probe::dump::{self, DumpFailureKind};
use sprig_cluster::probe::{ProbeDepth, Prober};
use sprig_cluster::profile::{DeriveInput, derive};
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::NodeId;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.trim().is_empty())
}

/// An endpoint as `Credential::endpoint` builds it: TCP and TLS, verified
/// against `ca` (or not verified, or no TLS at all).
fn endpoint(url: &str, tls: Option<Option<&str>>) -> NodeEndpoint {
    let mut b = OptsBuilder::from_opts(Opts::from_url(url).unwrap()).prefer_socket(false);
    b = match tls {
        Some(Some(ca)) => {
            b.ssl_opts(SslOpts::default().with_root_certs(vec![PathBuf::from(ca).into()]))
        }
        Some(None) => b.ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ),
        None => b,
    };
    NodeEndpoint {
        node: NodeId::new("n1").unwrap(),
        opts: Opts::from(b),
    }
}

#[tokio::test]
async fn tls_sources_pass_the_dump_probe_and_derive_feed_source() {
    let (Some(repl), Some(root), Some(ca)) = (
        var("SPRIG_TEST_BINLOG_TLS"),
        var("SPRIG_TEST_BINLOG_TLS_ROOT"),
        var("SPRIG_TEST_BINLOG_TLS_CA"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_BINLOG_TLS, _ROOT and _CA are not set");
        return;
    };
    let t = Duration::from_secs(5);
    // Verified against the CA: passes.
    let p = dump::run(&endpoint(&repl, Some(Some(&ca))), "", t).await;
    assert!(p.passed(), "{:?}", p.failure);
    assert!(p.server_version.is_some());
    // skip_verify: passes.
    let p = dump::run(&endpoint(&repl, Some(None)), "", t).await;
    assert!(p.passed(), "{:?}", p.failure);
    // No TLS: the server refuses; a conclusive transport failure whose fix
    // names the credential's TLS.
    let p = dump::run(&endpoint(&repl, None), "", t).await;
    let f = p.failure.clone().expect("plain text is refused");
    assert_eq!(f.kind, DumpFailureKind::Transport);
    assert!(f.fix.contains("tls"), "{}", f.fix);
    assert!(p.blocks_feed());
    // A CA that didn't sign the certificate: also Transport, not Unsupported.
    if let Some(other) = var("SPRIG_TEST_BINLOG_TLS_OTHER_CA") {
        let p = dump::run(&endpoint(&repl, Some(Some(&other))), "", t).await;
        assert_eq!(p.failure.unwrap().kind, DumpFailureKind::Transport);
    }

    // The whole deep probe and the derived profile: feed and mover sources.
    let ctl = [endpoint(&root, Some(Some(&ca)))];
    let repl_eps = [endpoint(&repl, Some(Some(&ca)))];
    let probes = Prober::new(Duration::from_secs(10))
        .probe_cluster_with_repl(
            TopologyKind::SingleNode,
            &ctl,
            Some(&repl_eps),
            ProbeDepth::Deep,
        )
        .await;
    let profile = derive(DeriveInput {
        topology: TopologyKind::SingleNode,
        probes: &probes,
        declared: None,
        previous: None,
    });
    eprintln!(
        "feed_level={:?} feed_source={} mover_source={}",
        profile.derived.feed_level, profile.derived.feed_source, profile.derived.mover_source
    );
    assert!(profile.derived.feed_source && profile.derived.mover_source);
}
