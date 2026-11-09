//! Bolt state-machine tests (chapter 43, "Testing"): the server over an
//! in-memory service, driven by the crate's own client through a duplex
//! pipe.

#![allow(clippy::many_single_char_names)]

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_bolt::client::BoltClient;
use sprig_proto_bolt::server::{
    BoltFailure, BoltService, BoltSession, Config, ConnInfo, End, QueryResult, serve,
};
use sprig_proto_bolt::{PackValue, Request, Response};

struct Svc;

struct Sess {
    in_tx: bool,
    log: Vec<String>,
}

#[async_trait]
impl BoltService for Svc {
    async fn logon(
        &self,
        _info: &ConnInfo,
        auth: &PackValue,
    ) -> Result<Box<dyn BoltSession>, BoltFailure> {
        if auth.get("scheme").and_then(PackValue::as_str) != Some("basic") {
            return Err(BoltFailure::new(
                "Neo.ClientError.Security.Unauthorized",
                "scheme",
            ));
        }
        if auth.get("credentials").and_then(PackValue::as_str) != Some("pw") {
            return Err(BoltFailure::new(
                "Neo.ClientError.Security.Unauthorized",
                "bad credentials",
            ));
        }
        Ok(Box::new(Sess {
            in_tx: false,
            log: vec![],
        }))
    }
}

#[async_trait]
impl BoltSession for Sess {
    async fn run(
        &mut self,
        query: &str,
        _params: &PackValue,
        _extra: &PackValue,
    ) -> Result<QueryResult, BoltFailure> {
        self.log.push(query.to_owned());
        if query == "BAD" {
            return Err(BoltFailure::new(
                "Neo.ClientError.Statement.SyntaxError",
                "bad",
            ));
        }
        let n: i64 = query.parse().unwrap_or(1);
        Ok(QueryResult {
            fields: vec!["x".into()],
            records: (0..n)
                .map(|i| vec![PackValue::Int(i)])
                .collect::<VecDeque<_>>(),
            qtype: "r",
            ..QueryResult::default()
        })
    }
    async fn begin(&mut self, _extra: &PackValue) -> Result<(), BoltFailure> {
        self.in_tx = true;
        Ok(())
    }
    async fn commit(&mut self) -> Result<Option<String>, BoltFailure> {
        self.in_tx = false;
        Ok(Some("SB1:x".into()))
    }
    async fn rollback(&mut self) -> Result<(), BoltFailure> {
        self.in_tx = false;
        Ok(())
    }
    fn last_bookmark(&self) -> Option<String> {
        Some("SB1:auto".into())
    }
    async fn reset(&mut self) {
        self.in_tx = false;
    }
    async fn route(
        &mut self,
        _routing: &PackValue,
        _extra: &PackValue,
    ) -> Result<PackValue, BoltFailure> {
        Ok(PackValue::map([("ttl", PackValue::Int(300))]))
    }
}

async fn start(
    proposals: Option<[[u8; 4]; 4]>,
) -> (
    BoltClient<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<End>,
) {
    let (a, b) = tokio::io::duplex(1 << 20);
    let svc = Arc::new(Svc);
    let h = tokio::spawn(async move {
        serve(
            b,
            "127.0.0.1:1".parse().expect("addr"),
            false,
            None,
            svc.as_ref(),
            &Config::default(),
            CancellationToken::new(),
        )
        .await
        .expect("serve")
    });
    let c = BoltClient::handshake(a, proposals)
        .await
        .expect("handshake");
    (c, h)
}

#[tokio::test]
async fn login_run_pull_and_transactions() {
    let (mut c, h) = start(None).await;
    assert_eq!(c.version.to_string(), "5.4");
    c.login("u", "pw").await.expect("login");
    assert_eq!(
        c.server.get("server").and_then(PackValue::as_str),
        Some("Neo4j/5.26.0")
    );
    let r = c.query("3", vec![], vec![]).await.expect("run");
    assert_eq!(r.fields, vec!["x"]);
    assert_eq!(r.records.len(), 3);
    assert_eq!(
        r.summary.get("bookmark").and_then(PackValue::as_str),
        Some("SB1:auto")
    );
    // A failure, then RESET (done by the client) and a working session.
    let e = c.query("BAD", vec![], vec![]).await.expect_err("bad");
    assert_eq!(e.code, "Neo.ClientError.Statement.SyntaxError");
    assert_eq!(
        c.query("1", vec![], vec![])
            .await
            .expect("after reset")
            .records
            .len(),
        1
    );
    // PULL in batches.
    c.send(&Request::Run {
        query: "5".into(),
        params: PackValue::Map(vec![]),
        extra: PackValue::Map(vec![]),
    })
    .await
    .expect("send");
    assert!(matches!(
        c.recv().await.expect("run ok"),
        Response::Success(_)
    ));
    c.send(&Request::Pull(PackValue::map([("n", PackValue::Int(2))])))
        .await
        .expect("send");
    let mut recs = 0;
    loop {
        match c.recv().await.expect("recv") {
            Response::Record(_) => recs += 1,
            Response::Success(m) => {
                assert_eq!(m.get("has_more"), Some(&PackValue::Bool(true)));
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(recs, 2);
    c.send(&Request::Discard(PackValue::map([(
        "n",
        PackValue::Int(-1),
    )])))
    .await
    .expect("send");
    assert!(matches!(
        c.recv().await.expect("discard"),
        Response::Success(_)
    ));
    // Explicit transaction with two results and qids.
    c.begin(vec![]).await.expect("begin");
    c.send(&Request::Run {
        query: "2".into(),
        params: PackValue::Map(vec![]),
        extra: PackValue::Map(vec![]),
    })
    .await
    .expect("send");
    let Response::Success(m) = c.recv().await.expect("run") else {
        panic!("run")
    };
    assert_eq!(m.get("qid"), Some(&PackValue::Int(0)));
    let r = c.query("4", vec![], vec![]).await.expect("second");
    assert_eq!(r.records.len(), 4);
    let b = c.commit().await.expect("commit");
    assert_eq!(b.get("bookmark").and_then(PackValue::as_str), Some("SB1:x"));
    // ROUTE answers the table.
    let rt = c.route(None).await.expect("route");
    assert_eq!(
        rt.get("rt").and_then(|t| t.get("ttl")),
        Some(&PackValue::Int(300))
    );
    // Messages in the wrong state: COMMIT outside a transaction fails, then
    // everything but RESET is IGNORED.
    c.send(&Request::Commit).await.expect("send");
    assert!(matches!(
        c.recv().await.expect("f"),
        Response::Failure { .. }
    ));
    c.send(&Request::Pull(PackValue::Map(vec![])))
        .await
        .expect("send");
    assert_eq!(c.recv().await.expect("ignored"), Response::Ignored);
    c.reset().await.expect("reset");
    c.goodbye().await.expect("bye");
    assert!(matches!(
        h.await.expect("join"),
        End::Defunct(_) | End::Closed
    ));
}

#[tokio::test]
async fn bad_credentials_close_the_connection() {
    let (mut c, h) = start(None).await;
    let e = c.login("u", "nope").await.expect_err("refused");
    assert_eq!(e.code, "Neo.ClientError.Security.Unauthorized");
    assert!(matches!(h.await.expect("join"), End::Defunct(_)));
}

#[tokio::test]
async fn bolt_5_0_takes_credentials_in_hello() {
    let (mut c, _h) = start(Some([[0, 0, 0, 5], [0; 4], [0; 4], [0; 4]])).await;
    assert_eq!(c.version.to_string(), "5.0");
    c.login("u", "pw").await.expect("login");
    assert_eq!(
        c.query("1", vec![], vec![])
            .await
            .expect("run")
            .records
            .len(),
        1
    );
}

#[tokio::test]
async fn no_common_version_answers_zeros() {
    let (a, b) = tokio::io::duplex(1024);
    let svc = Svc;
    let h = tokio::spawn(async move {
        serve(
            b,
            "127.0.0.1:1".parse().expect("addr"),
            false,
            None,
            &svc,
            &Config::default(),
            CancellationToken::new(),
        )
        .await
        .expect("serve")
    });
    let e = BoltClient::handshake(a, Some([[0, 0, 4, 4], [0; 4], [0; 4], [0; 4]]))
        .await
        .expect_err("refused");
    assert!(e.message.contains("no Bolt version"));
    assert_eq!(h.await.expect("join"), End::NoVersion);
}
